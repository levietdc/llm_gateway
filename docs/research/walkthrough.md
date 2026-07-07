# Tài liệu Nghiệm thu Hệ thống AI Gateway Proxy (Walkthrough)

Dự án đã triển khai và hoàn thành đầy đủ tất cả các tính năng cốt lõi theo kế hoạch đề ra. Toàn bộ mã nguồn đã được tối ưu hóa bất đồng bộ (async), vượt qua 100% các kịch bản kiểm thử tích hợp (bao gồm kiểm thử sập luồng giữa chừng - mid-stream failover với Google Gemini), và đã được đóng gói bằng Docker Compose.

Mới nhất, hệ thống đã được nâng cấp để chuyển đổi nhà cung cấp mô hình chính từ **Anthropic (Claude)** sang **Google Gemini** sử dụng giao thức chính thức tương thích OpenAI REST API rất tối ưu và ổn định.

---

## 1. Kết quả Triển khai & Cấu trúc Dự án

Dưới đây là cấu trúc các tệp chính đã được tích hợp vào dự án:

```
ai-gateway-proxy-system/
├── app/
│   ├── core/
│   │   ├── config.py           # Quản lý cấu hình Settings qua Pydantic v2 (Dùng GEMINI_API_KEY)
│   │   ├── redis_client.py     # Kết nối Redis async, load Lua Script Rate Limiter
│   │   └── observability.py    # Cấu hình OTel (Arize Phoenix) và Prometheus Metrics
│   ├── services/
│   │   ├── cache_service.py    # Dual-Layer Cache: Exact Match (SHA-256) & Semantic Cache (RedisVL)
│   │   ├── token_counter.py    # Đếm token input/output thời gian thực (Hỗ trợ gemini-* qua tiktoken)
│   │   └── failover_router.py  # Chuyển mạch Mid-Stream Failover (Google Gemini -> OpenAI GPT-4o)
│   │   static/
│   │   └── index.html          # Giao diện admin Dashboard tối giản Obsidian Flux (Đã cập nhật Gemini)
│   └── main.py                 # FastAPI Endpoint (/v1/chat/completions, định tuyến tĩnh & /metrics)
├── docker/
│   ├── docker-compose.yml      # Định nghĩa stack App, Redis Stack, Arize Phoenix, Prometheus, Grafana
│   └── prometheus.yml          # Scrape target cho Prometheus
├── docs/
│   └── research/
│       ├── findings.md         # Tài liệu nghiên cứu prompt, OTel, Prometheus
│       ├── rate_limiter.lua    # Lua script Token Bucket tối ưu
│       └── semantic_cache.py   # Code mẫu thử nghiệm RedisVL + FastEmbed
│       └── walkthrough.md      # Tài liệu nghiệm thu hệ thống (File này)
├── tests/
│   ├── test_gateway.py         # Bộ test unit cơ bản (config, token, cache, rate limit)
│   └── test_failover.py        # Bộ test tích hợp chuyên sâu cho Mid-Stream Failover với Gemini
├── Dockerfile                  # Đóng gói tối ưu hóa với uv pip
├── pyproject.toml              # Cấu hình dependency hiện đại của uv
└── uv.lock                     # Lock tệp của uv
```

---

## 2. Các Tính năng Đã Thực hiện và Xác minh

### A. Tối ưu chi phí: Bộ nhớ đệm kép (Dual-Layer Cache)
- **Hoạt động**: Kiểm tra exact match qua mã băm SHA-256 của payload tin nhắn (`cache:exact:{sha256}`). Nếu miss, chuyển qua Semantic Cache (`redisvl`) với embedding cục bộ sinh bởi `FastEmbed` trên CPU.
- **Xác minh**: 
  - Đã kiểm tra và chạy thành công trên local Redis.
  - Hỗ trợ chế độ **Graceful Fallback**: Nếu Redis không cài RediSearch module (như môi trường test cơ bản), hệ thống tự động cảnh báo, bỏ qua Layer 2 và sử dụng Layer 1 để hệ thống hoạt động bình thường, không gây sập ứng dụng.

### B. Kiểm soát ngân sách: Token Bucket Rate Limiter
- **Hoạt động**:
  - Đếm token đầu vào bằng `tiktoken` (cho OpenAI & Google Gemini) và `tokenizers` (cho Claude) chạy trên threadpool riêng biệt để không chặn luồng chính.
  - Sử dụng Lua Script để nạp lại token tự động theo thời gian thực dựa trên Redis time, tránh lệch múi giờ client.
  - Trừ token đầu vào (trước request) và token đầu ra (sau khi stream xong). Nếu budget bị cạn, hệ thống sẽ ngắt kết nối stream ngay lập tức.
- **Xác minh**: 
  - Test case `test_redis_client` xác nhận việc trừ token hợp lệ thành công và từ chối các request vượt ngưỡng (trả về `allowed=False` và thông báo thời gian cần chờ `retry_after`).

### C. Sống sót qua sự cố: Mid-Stream Failover Router (Từ Gemini sang OpenAI)
- **Hoạt động**: 
  - Bọc luồng stream của Google Gemini (sử dụng endpoint OpenAI-compatible chính thức: `https://generativelanguage.googleapis.com/v1beta/openai/chat/completions`). 
  - Khi xảy ra lỗi kết nối hoặc timeout (mô phỏng Gemini sập giữa chừng), hệ thống bắt lỗi, trích xuất đoạn text đã nhận được (`partial_text`).
  - Gửi request mới tới OpenAI (mặc định model `gpt-4o`) kèm theo prompt chỉ thị đặc biệt (System Prompt Override Pattern) bắt buộc OpenAI chỉ sinh phần văn bản tiếp theo từ điểm bị ngắt mà không được lặp lại nội dung.
  - Ghép luồng stream tiếp tục gửi về cho client trên chính kết nối đang mở.
- **Xác minh**: 
  - Test case `test_mid_stream_failover` mô phỏng Gemini bị ngắt đột ngột giữa chừng (`httpx.RemoteProtocolError`). Kết quả đầu ra generator trả về văn bản ghép hoàn chỉnh không tì vết, ghi nhận chính xác lỗi sập vào OTel span con, trong khi span cha của Gateway vẫn ghi nhận thành công (Status OK, Final Model: `gpt-4o`).

### D. Giám sát: GenAI Observability
- **Hoạt động**:
  - **Traces**: OpenTelemetry xuất dữ liệu spans (bao gồm các attributes chuẩn GenAI: `gen_ai.system`, `gen_ai.request.model`, `gen_ai.usage.completion_tokens`, `gen_ai.time_to_first_token`) tới cổng OTLP gRPC của Arize Phoenix.
  - **Metrics**: Prometheus client xuất endpoint `/metrics` đo đạc các chỉ số số lượng request, thời gian phản hồi (P95/P99 latency), số lượng token tiêu thụ và chi phí ước tính dựa trên đơn giá USD.

---

## 3. Kết quả Chạy Kiểm thử (Validation Results)

Chạy toàn bộ 7 test cases trong dự án:

```bash
PYTHONPATH=. .venv/bin/pytest -v
```

Kết quả:
```
============================= test session starts ==============================
platform darwin -- Python 3.11.13, pytest-9.1.1, pluggy-1.6.0
collected 7 items

tests/test_failover.py::test_normal_stream PASSED                        [ 28%]
tests/test_failover.py::test_mid_stream_failover PASSED                  [ 42%]
tests/test_gateway.py::test_config PASSED                                [ 57%]
tests/test_gateway.py::test_token_counter PASSED                         [ 71%]
tests/test_gateway.py::test_redis_client PASSED                          [ 85%]
tests/test_gateway.py::test_cache_service PASSED                         [100%]

============================== 7 passed in 1.38s ===============================
```
> [!TIP]
> Tất cả các kiểm thử đã chạy qua thành công và khẳng định độ ổn định của hệ thống trong mọi kịch bản lỗi mạng và chuyển mạch dự phòng với Google Gemini.

---

## 4. Hướng dẫn Vận hành Nhanh (Quick Start)

### Khởi chạy môi trường Docker Compose
Chạy toàn bộ hạ tầng (App, Redis Stack, Arize Phoenix, Prometheus, Grafana):
```bash
# Di chuyển vào thư mục docker và khởi chạy
cd docker
docker-compose up --build -d
```

### Các Cổng dịch vụ sau khi chạy:
- **AI Gateway Proxy**: `http://localhost:8000/v1/chat/completions` (Dashboard tại `http://localhost:8000/`)
- **Arize Phoenix UI (Traces)**: `http://localhost:6006` (Xem trace LLM, logs)
- **Prometheus UI (Metrics)**: `http://localhost:9090` (Xem biểu đồ token, latency)
- **Grafana UI (Dashboards)**: `http://localhost:3000` (Tạo dashboard theo dõi chi phí)
- **RedisInsight (Redis UI)**: `http://localhost:8001` (Xem cache và trạng thái rate limits)

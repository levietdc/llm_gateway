# Thiết kế Dashboard Giám sát & Kiểm thử AI Gateway (Obsidian Flux)

Hệ thống điều khiển và giám sát trực quan (Dashboard) cho AI Gateway Proxy được thiết kế dựa trên ngôn ngữ **Obsidian Flux** (Thiết kế thiên hướng tối - Dark Mode, độ tương phản cao, phong cách kỹ thuật Glassmorphism). Dashboard giúp các kỹ sư dễ dàng kiểm thử các kịch bản chuyển mạch (Failover), giả lập lỗi, đồng thời theo dõi các chỉ số trễ, token, và chi phí theo thời gian thực.

---

## 1. Ảnh chụp Bản Thiết kế giao diện (Stitch MCP)

Dưới đây là hình ảnh mockup độ phân giải cao được sinh tự động bởi Stitch MCP:

![Bản thiết kế Dashboard](/Users/leviet/.gemini/antigravity/brain/0293d8f7-7f63-489e-864f-9b3e2322c51a/dashboard_mockup.png)

---

## 2. Chi tiết Cấu trúc và Khối Chức năng

Giao diện được chia thành **3 cột (Three-Column Layout)** để tối ưu hóa mật độ hiển thị dữ liệu kỹ thuật:

### Cột 1: Thanh Điều hướng (Sidebar Navigation - Trái)
- **AGY AI Gateway**: Logo thương hiệu cùng trạng thái vận hành chung của hệ thống.
- **Menu chức năng**:
  - **Dashboard (Active)**: Xem biểu đồ tổng quan và metrics.
  - **Test Console**: Giao diện gửi prompt và kiểm thử kết nối trực tiếp.
  - **Semantic Cache Explorer**: Tra cứu các truy vấn tương đồng và kiểm tra tỉ lệ hit cache.
  - **Rate Limits**: Cấu hình budget/token bucket cho từng nhóm API Key.
  - **Telemetry Logs**: Xem danh sách spans và traces thô của OpenTelemetry.

### Cột 2: Bảng Điều khiển & Kiểm thử (System Testing Console - Giữa)
- **Trạng thái hệ thống (Badges)**: Các đèn LED hiển thị kết nối của các thành phần lõi:
  - `Redis: Connected` (Xanh lá)
  - `Phoenix: Connected` (Xanh lá)
  - `Anthropic: Active` (Xanh dương)
  - `OpenAI: Active` (Xanh dương)
- **Cấu hình định tuyến (Routing Configuration)**:
  - **Primary Model**: Claude-3-5-sonnet (Nhà cung cấp chính).
  - **Backup Model**: GPT-4o-mini (Nhà cung cấp dự phòng).
- **Bộ giả lập tham số (Simulation parameters)**:
  - Ô nhập **User Key** (ví dụ: `user_demo_123`) để test tính năng rate limiter và trừ budget.
  - Thanh trượt **Temperature** và **Max Tokens**.
- **Kịch bản lỗi chủ động (Trigger Failure)**:
  - Một nút gạt (Toggle Switch): **"Simulate Mid-Stream Connection Error"** (Đồng màu tím Violet để nhấn mạnh). Khi bật nút này, hệ thống sẽ cố tình làm đứt luồng stream của Anthropic để kích hoạt chuyển mạch sang OpenAI.
- **Khung Chat & Kết quả (Chat & Console)**:
  - Khung nhập câu hỏi và nút **Send Request (Stream)**.
  - Khung **Output Terminal** hiển thị dòng chữ chạy ra dưới dạng code (JetBrains Mono).
  - Overlay thông báo đặc biệt: `Streaming từ: Anthropic (Claude-3.5) -> Lỗi kết nối -> Tiếp nối bằng: OpenAI (GPT-4o)`.

### Cột 3: Phân tích & Đo lường (Analytics & Metrics - Phải)
- **Grid Chỉ số Thời gian thực (Scraped từ Prometheus)**:
  - **TTFT (Time-To-First-Token)**: `240ms` (P95: `350ms`) - Đo lường độ nhạy của luồng stream.
  - **Cache Hit Rate**: `42.5%` (Trong đó: Exact match `25%`, Semantic `17.5%`) - Highlight màu xanh lá lục bảo.
  - **Budget / Tokens**: `8,420 tokens remaining` (Thanh progress hiển thị lượng token còn lại trong Bucket của User).
  - **Estimated Cost Saved**: `$14.28` (Số tiền tiết kiệm được nhờ hit cache).
- **Recent Traces (Danh sách Trace gần đây)**:
  - Hiển thị danh sách các request đi qua Gateway dưới dạng các thanh biểu đồ ngang thể hiện độ trễ (latency).
  - Phân loại màu sắc trực quan: *Xanh lá* (Truy cập từ cache), *Tím* (Request bị failover), *Xanh dương* (Request chạy trực tiếp bình thường).
- **Arize Phoenix Deep Link**: Nút **"Open Trace in Phoenix"** để nhảy trực tiếp sang giao diện debug traces chi tiết của Phoenix OTel.

---

## 3. Cách thức Tích hợp Dashboard vào Codebase

Để Dashboard này hoạt động thực tế với backend FastAPI hiện tại:
1. **API Endpoints**: Dashboard sẽ gọi trực tiếp endpoint `/v1/chat/completions` (sử dụng SSE client để nhận stream và hiển thị lên Output Console).
2. **Prometheus Metrics**: Gọi endpoint `/metrics` định kỳ mỗi 2-5 giây để phân tích cú pháp (parse) các metrics như `llm_time_to_first_token_seconds`, `llm_requests_total`, và `llm_cost_total` để vẽ đồ thị.
3. **Failover Simulation**: Backend hỗ trợ một query parameter hoặc header đặc biệt là `X-Simulate-Failover: true`. Khi nhận header này, `failover_router.py` sẽ chủ động ngắt luồng Anthropic sau 5-10 tokens đầu tiên để kiểm thử khả năng chuyển mạch của hệ thống.

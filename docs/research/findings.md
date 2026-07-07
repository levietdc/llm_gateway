# AI Gateway Proxy Research: Observability and Mid-Stream Failover

This document outlines the technical research and implementation guidelines for the AI Gateway Proxy project. It covers two primary areas:
1. **Observability**: OpenTelemetry instrumentation with Arize Phoenix (traces) and Prometheus (metrics) for LLM streaming.
2. **Failover Resilience**: Prompt engineering strategies to achieve seamless mid-stream failover from Anthropic to OpenAI.

---

## 1. Observability Architecture

The AI Gateway Proxy handles streaming requests from users and routes them to upstream LLM providers (e.g., Anthropic, OpenAI). To monitor performance, errors, usage, and latency, we deploy a dual-observability pipeline:
* **Arize Phoenix** (via OpenTelemetry Traces): Used for request tracing, span attributes, debugging, and GenAI evaluations.
* **Prometheus** (via OpenTelemetry Metrics): Used for high-frequency time-series metrics (latencies, token counts, request rates) and dashboarding.

### Flow Architecture

```mermaid
graph TD
    Client[Client App] -->|Stream Request| Gateway[AI Gateway Proxy]
    Gateway -->|1. LLM API Call| Provider[Upstream LLM: Anthropic/OpenAI]
    Gateway -->|2. Export Traces (OTLP/gRPC)| Phoenix[Arize Phoenix]
    Gateway -->|3. Expose Metrics| PromScrape[Prometheus Scraper]
    PromScrape -->|Scrapes /metrics| PromServer[Prometheus Server]
    
    subgraph Gateway Observability
        OTelSDK[OpenTelemetry SDK]
        Tracer[OTel Tracer] -->|OTLPSpanExporter| Phoenix
        Meter[OTel Meter] -->|PrometheusMetricReader| PromScrape
    end
```

---

## 2. OpenTelemetry & Arize Phoenix Setup Guidelines

To capture semantic traces for GenAI operations, we adhere to the **OpenTelemetry GenAI Semantic Conventions**. This ensures standard attributes are propagated to Arize Phoenix.

### Dependencies
Ensure the following packages are installed (matching `requirements.txt`):
```bash
pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp arize-phoenix-otel
```

### Python SDK Initialization
Create an initialization helper (e.g., `app/core/observability.py`) to bootstrap tracing:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

def init_tracer(service_name: str = "ai-gateway-proxy", phoenix_endpoint: str = "http://localhost:4317") -> trace.Tracer:
    """
    Initializes the OpenTelemetry Tracer Provider with an OTLP exporter 
    configured to send spans to Arize Phoenix.
    """
    # Create resource identifying our service
    resource = Resource.create(attributes={
        "service.name": service_name,
        "environment": "production"
    })
    
    provider = TracerProvider(resource=resource)
    
    # Configure OTLP gRPC exporter for Phoenix
    # Arize Phoenix listens on port 4317 for OTLP gRPC by default
    exporter = OTLPSpanExporter(endpoint=phoenix_endpoint, insecure=True)
    
    # Use BatchSpanProcessor for production performance
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    
    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name)
```

### GenAI Semantic Conventions Attributes
When tracing LLM spans, always include these standard attributes to ensure Phoenix parses them correctly:

| Attribute Name | Description | Example |
| :--- | :--- | :--- |
| `gen_ai.system` | The LLM system name | `"openai"`, `"anthropic"` |
| `gen_ai.request.model` | The requested model name | `"claude-3-5-sonnet-20240620"` |
| `gen_ai.response.model` | The actual model that responded | `"gpt-4o-2024-05-13"` |
| `gen_ai.request.temperature` | Temperature parameter | `0.7` |
| `gen_ai.request.max_tokens` | Maximum token limit | `4096` |
| `gen_ai.usage.prompt_tokens` | Count of input tokens | `150` |
| `gen_ai.usage.completion_tokens`| Count of generated tokens | `320` |

---

## 3. Prometheus Metrics Setup Guidelines

To collect operational metrics without adding latency, we configure an OpenTelemetry `PrometheusMetricReader` inside our FastAPI application.

### Key Metrics to Track
We define the following metrics for the AI Gateway:
1. `llm_request_duration_seconds` (Histogram): Total roundtrip latency of LLM calls.
2. `llm_time_to_first_token_seconds` (Histogram): Time elapsed from prompt sent until the first response token is streamed.
3. `llm_tokens_total` (Counter): Cumulative count of prompt and completion tokens.
4. `llm_requests_total` (Counter): Total number of gateway requests, partitioned by status (success/failure/failover).

### FastAPI Prometheus Integration Code

```python
from fastapi import FastAPI
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import make_asgi_app

app = FastAPI()

# 1. Initialize Prometheus Metric Reader
# This automatically registers OTel metric collection with the prometheus_client library
metric_reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[metric_reader])
metrics.set_meter_provider(provider)

# 2. Get the meter instance
meter = metrics.get_meter("ai-gateway-metrics")

# 3. Define metrics instruments
request_counter = meter.create_counter(
    name="llm_requests_total",
    description="Total count of LLM requests processed by the gateway",
    unit="1"
)

ttft_histogram = meter.create_histogram(
    name="llm_time_to_first_token_seconds",
    description="Duration in seconds until the first chunk of token is received",
    unit="s"
)

# 4. Mount Prometheus metrics endpoint
# This exposes a standard WSGI/ASGI endpoint at /metrics that Prometheus scrapes
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
```

---

## 4. Instrumenting LLM Streaming

Streaming responses present a challenge for observability because:
1. Spans must remain open until the *entire* stream terminates.
2. Metrics like Time-to-First-Token (TTFT) require measuring interval deltas across async iterations.
3. Usage details (token counts) might not be returned inside the stream chunks (especially in older provider APIs).

### Streaming Wrapper Logic
Below is a code pattern demonstrating how to wrap an asynchronous streaming response to capture spans, TTFT, and total tokens.

```python
import time
from typing import AsyncGenerator
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

tracer = trace.get_tracer("ai-gateway")

async def instrumented_llm_stream(
    provider_name: str,
    model_name: str,
    raw_stream_generator: AsyncGenerator[str, None]
) -> AsyncGenerator[str, None]:
    """
    Wraps an upstream async generator stream to inject OpenTelemetry tracking,
    calculate TTFT, track total stream latency, and count approximate tokens.
    """
    # 1. Start OTel Span
    span = tracer.start_span(
        name=f"llm.stream.{provider_name}",
        attributes={
            "gen_ai.system": provider_name,
            "gen_ai.request.model": model_name,
        }
    )
    
    start_time = time.time()
    first_token_received = False
    token_count = 0
    
    try:
        with trace.use_span(span, end_on_exit=False):
            async for chunk in raw_stream_generator:
                if not first_token_received:
                    # 2. Calculate and record TTFT
                    ttft = time.time() - start_time
                    span.set_attribute("gen_ai.time_to_first_token", ttft)
                    # Expose to Prometheus (conceptually)
                    # ttft_histogram.record(ttft, {"model": model_name})
                    first_token_received = True
                
                # Yield chunk immediately to user (avoid buffering latency)
                yield chunk
                
                # 3. Track tokens (rough estimation if not present in chunk metadata)
                # Typically, 1 word ~= 1.3 tokens. In production, use tiktoken or tokenizers.
                token_count += len(chunk.split())
            
            # 4. Close out span variables on successful completion
            total_duration = time.time() - start_time
            span.set_attribute("gen_ai.usage.completion_tokens", int(token_count * 1.3))
            span.set_attribute("gen_ai.duration", total_duration)
            span.set_status(Status(StatusCode.OK))
            
    except Exception as e:
        # Record error details on the span
        span.record_exception(e)
        span.set_status(Status(StatusCode.ERROR, str(e)))
        raise e
    finally:
        # Ensure the span is always closed
        span.end()
```

---

## 5. Mid-Stream Failover Prompt Design

When an Anthropic (Claude) stream fails mid-generation, the AI Gateway Proxy intercepts the failure and silently resumes generation using an OpenAI model (e.g., GPT-4o). The goal is to stitch the two output chunks seamlessly so the user's client app sees a single unified output.

### The Challenge
If you simply prompt OpenAI with the original query, it will write the entire response from scratch. If you append the partial response as an `assistant` role message, standard OpenAI models often treat it as a *completed* turn in a chat conversation and will not proceed to finish it, or they will output conversational prefaces like *"Sure! Here is the rest of that text..."*.

### The System Prompt Override Pattern
To force OpenAI to act as a seamless continuator, the Gateway overrides the messages array sent to OpenAI.

#### 1. Clean the Truncated Response
Before constructing the prompt:
* Locate the last text character of the truncated Anthropic output.
* If it cuts off mid-word (e.g., `"The primary factor is photosyn"`), trim the trailing incomplete letters back to the last whitespace or complete word if possible, OR instruct the model explicitly to complete the word. (Tuning the instruction to complete the word is safer).

#### 2. Format the OpenAI Request
Construct a special message history containing the continuation instruction. 

```json
[
  {
    "role": "system",
    "content": "You are a continuation assistant. Your ONLY job is to seamlessly continue a response that was abruptly cut off mid-sentence. We will supply you with: (1) The Original User Prompt, and (2) The Truncated Partial Response generated so far. You must start generating output from the EXACT CHARACTER where the Truncated Partial Response leaves off. Do NOT repeat any part of the Truncated Partial Response. Do NOT add any introductory transition words, conversational filler (e.g., 'Sure, here is the continuation', 'Continuing:'), or wrapping quotes. Write ONLY the text necessary to complete the response naturally."
  },
  {
    "role": "user",
    "content": "[ORIGINAL USER PROMPT]\nWhat are the steps of photosynthesis and why is light required?\n\n[TRUNCATED PARTIAL RESPONSE]\nPhotosynthesis occurs in two primary stages: the light-dependent reactions and the light-independent reactions (Calvin cycle). During the light-dependent reactions, chlorophyll absorbs light energy, which excited electrons. This energy is used to split water molecules, releasing oxygen as a byproduct and generating ATP and NADPH. In the Calvin cycle, carbon dioxide is captured to synthesize glucose. Light is fundamentally required because"
  }
]
```

### Prompt Engineering Guidelines

1. **Strict Output Guardrails**: Use uppercase instructions like `ONLY`, `EXACT CHARACTER`, and `Do NOT repeat` to prevent the continuation model from hallucinating a intro phrase.
2. **Temperature Configuration**: Set `temperature=0.2` or lower for the continuation request to maximize coherence and logical follow-through of the original output structure.
3. **Stitching Mechanics at the Proxy**:
   When the proxy receives the first token chunk from OpenAI:
   * Do not insert a space or newline.
   * Directly append the new stream to the truncated end-point.
   * If the truncated response ended with `"because"`, and OpenAI responds with `" it provides the necessary activation energy..."`, the proxy yields the first chunk verbatim to merge into `"because it provides the necessary..."`.

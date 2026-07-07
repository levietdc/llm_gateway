import os
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from prometheus_client import Counter, Histogram

logger = logging.getLogger(__name__)

# --- Prometheus Metrics ---

llm_requests_total = Counter(
    "llm_requests_total",
    "Total count of LLM requests processed by the gateway",
    ["status", "model", "provider"]  # status: success, failure, failover
)

llm_request_duration_seconds = Histogram(
    "llm_request_duration_seconds",
    "Total roundtrip latency of LLM calls in seconds",
    ["model", "provider"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
)

llm_time_to_first_token_seconds = Histogram(
    "llm_time_to_first_token_seconds",
    "Duration in seconds until the first chunk of token is received",
    ["model", "provider"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0)
)

llm_tokens_total = Counter(
    "llm_tokens_total",
    "Cumulative count of prompt and completion tokens",
    ["type", "model", "provider"]  # type: prompt, completion
)

llm_cost_total = Counter(
    "llm_cost_total",
    "Estimated cost of LLM requests in USD",
    ["model", "provider"]
)

cache_requests_total = Counter(
    "cache_requests_total",
    "Total cache requests partitioned by layer and outcome",
    ["layer", "outcome"]  # layer: exact, semantic; outcome: hit, miss
)

# --- OpenTelemetry / Arize Phoenix Setup ---

tracer: trace.Tracer | None = None

def init_tracer(service_name: str = "ai-gateway-proxy") -> trace.Tracer:
    """
    Initializes the OpenTelemetry Tracer Provider with an OTLP/gRPC exporter
    configured to send spans to Arize Phoenix.
    """
    global tracer
    phoenix_endpoint = os.getenv("PHOENIX_ENDPOINT", "http://localhost:4317")
    
    logger.info(f"Initializing OpenTelemetry Tracer targeting Arize Phoenix at {phoenix_endpoint}...")
    
    resource = Resource.create(attributes={
        "service.name": service_name,
        "environment": os.getenv("ENVIRONMENT", "production")
    })
    
    provider = TracerProvider(resource=resource)
    
    try:
        # Default OTLP Exporter pointing to Phoenix collector
        exporter = OTLPSpanExporter(endpoint=phoenix_endpoint, insecure=True)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        tracer = trace.get_tracer(service_name)
        logger.info("OpenTelemetry tracer successfully initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry tracer: {e}. Falling back to default provider.")
        # If gRPC setup fails, fall back to global tracer provider
        trace.set_tracer_provider(provider)
        tracer = trace.get_tracer(service_name)
        
    return tracer

def get_tracer() -> trace.Tracer:
    """
    Returns the initialized tracer, or gets a basic one if not initialized yet.
    """
    global tracer
    if tracer is None:
        tracer = trace.get_tracer("ai-gateway-proxy")
    return tracer

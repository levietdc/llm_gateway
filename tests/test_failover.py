import pytest
import asyncio
import json
import time
import httpx
import contextlib
from unittest.mock import MagicMock, patch
from opentelemetry import trace
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from app.core.config import settings
from app.core.redis_client import redis_client
from app.services.cache_service import cache_service, compute_exact_hash
from app.services.failover_router import route_and_stream
from app.core.observability import (
    llm_requests_total,
    llm_request_duration_seconds,
    llm_time_to_first_token_seconds,
    llm_tokens_total,
    llm_cost_total,
)

# Helpers to read Prometheus metrics safely
def get_counter_value(counter, **labels):
    try:
        return counter.labels(**labels)._value.get()
    except Exception:
        return 0.0

def get_histogram_count(histogram, **labels):
    try:
        child = histogram.labels(**labels)
        for sample in child._child_samples():
            if sample.name == "_count":
                return sample.value
        return 0.0
    except Exception:
        return 0.0

# Session-scoped OTel setup to redirect spans to InMemorySpanExporter
@pytest.fixture(scope="module", autouse=True)
def setup_otel():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    
    old_provider = trace.get_tracer_provider()
    trace.set_tracer_provider(provider)
    
    # Explicitly override the tracer in the router module so it uses our provider
    import app.services.failover_router
    app.services.failover_router.tracer = provider.get_tracer("ai-gateway-proxy")
    
    yield exporter
    
    trace.set_tracer_provider(old_provider)

# Fixture to configure dummy keys (synchronous)
@pytest.fixture
def setup_keys(monkeypatch):
    monkeypatch.setattr(settings, "ANTHROPIC_API_KEY", "test-anthropic-key")
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "test-openai-key")

@pytest.mark.asyncio
async def test_normal_stream(setup_keys, setup_otel):
    exporter = setup_otel
    exporter.clear()
    
    # Connect Redis and initialize Cache
    await redis_client.connect()
    await cache_service.initialize()
    
    try:
        messages = [{"role": "user", "content": "What is the capital of France?"}]
        model = "claude-3-5-sonnet-20240620"
        temperature = 0.7
        
        # 1. Clean cache before test
        sha256_hash = compute_exact_hash(messages, model, temperature)
        exact_key = f"cache:exact:{sha256_hash}"
        if redis_client.client:
            await redis_client.client.delete(exact_key)
            await redis_client.client.delete("rate_limit:test-normal-user")
            
        # Get baseline metrics
        requests_before = get_counter_value(llm_requests_total, status="success", model=model, provider="anthropic")
        duration_count_before = get_histogram_count(llm_request_duration_seconds, model=model, provider="anthropic")
        ttft_count_before = get_histogram_count(llm_time_to_first_token_seconds, model=model, provider="anthropic")
        prompt_tokens_before = get_counter_value(llm_tokens_total, type="prompt", model=model, provider="anthropic")
        completion_tokens_before = get_counter_value(llm_tokens_total, type="completion", model=model, provider="anthropic")
        cost_before = get_counter_value(llm_cost_total, model=model, provider="anthropic")

        # Mock normal stream
        @contextlib.asynccontextmanager
        async def mock_normal_stream(method, url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            
            async def mock_aiter_lines():
                yield "event: content_block_delta"
                yield 'data: {"delta": {"text": "The capital "}}'
                yield "event: content_block_delta"
                yield 'data: {"delta": {"text": "of France "}}'
                yield "event: content_block_delta"
                yield 'data: {"delta": {"text": "is Paris."}}'
                
            response.aiter_lines = mock_aiter_lines
            yield response

        # Execute route_and_stream
        received_chunks = []
        with patch.object(httpx.AsyncClient, "stream", side_effect=mock_normal_stream):
            async for chunk in route_and_stream(messages, model, temperature, user_key="test-normal-user"):
                received_chunks.append(chunk)

        # 2. Verify chunks
        assert len(received_chunks) > 0
        response_text = ""
        for chunk in received_chunks:
            if chunk.startswith("data: "):
                data_str = chunk[len("data: "):].strip()
                if data_str == "[DONE]":
                    break
                data_json = json.loads(data_str)
                response_text += data_json["choices"][0]["delta"]["content"]
                
        assert response_text == "The capital of France is Paris."
        assert received_chunks[-1] == "data: [DONE]\n\n"

        # 3. Verify Cache matches the complete text
        cached_response = await cache_service.get_cached_response(messages, model, temperature)
        assert cached_response == "The capital of France is Paris."

        # 4. Verify Prometheus metrics
        requests_after = get_counter_value(llm_requests_total, status="success", model=model, provider="anthropic")
        duration_count_after = get_histogram_count(llm_request_duration_seconds, model=model, provider="anthropic")
        ttft_count_after = get_histogram_count(llm_time_to_first_token_seconds, model=model, provider="anthropic")
        prompt_tokens_after = get_counter_value(llm_tokens_total, type="prompt", model=model, provider="anthropic")
        completion_tokens_after = get_counter_value(llm_tokens_total, type="completion", model=model, provider="anthropic")
        cost_after = get_counter_value(llm_cost_total, model=model, provider="anthropic")

        assert requests_after == requests_before + 1
        assert duration_count_after == duration_count_before + 1
        assert ttft_count_after == ttft_count_before + 1
        assert prompt_tokens_after > prompt_tokens_before
        assert completion_tokens_after > completion_tokens_before
        assert cost_after > cost_before

        # 5. Verify OTel spans
        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        
        child_span = spans[0]
        parent_span = spans[1]
        
        assert child_span.name == "anthropic.messages.stream"
        assert child_span.status.status_code == StatusCode.OK
        assert child_span.attributes["gen_ai.request.model"] == model
        assert "gen_ai.time_to_first_token" in child_span.attributes
        
        assert parent_span.name == "chat.completions"
        assert parent_span.status.status_code == StatusCode.OK
        assert parent_span.attributes["gen_ai.request.model"] == model
        assert parent_span.attributes["gen_ai.response.model"] == model
        assert parent_span.attributes["gen_ai.system"] == "anthropic"
        assert parent_span.attributes["gen_ai.usage.completion_tokens"] > 0
        assert parent_span.attributes["gen_ai.usage.prompt_tokens"] > 0
        
        assert child_span.parent is not None
        assert child_span.parent.span_id == parent_span.context.span_id
    finally:
        await redis_client.disconnect()

@pytest.mark.asyncio
async def test_mid_stream_failover(setup_keys, setup_otel):
    exporter = setup_otel
    exporter.clear()
    
    # Connect Redis and initialize Cache
    await redis_client.connect()
    await cache_service.initialize()
    
    try:
        messages = [{"role": "user", "content": "Complete this story: Once upon a time..."}]
        model = "claude-3-5-sonnet-20240620"
        temperature = 0.7
        
        # Clean cache and rate limit
        sha256_hash = compute_exact_hash(messages, model, temperature)
        exact_key = f"cache:exact:{sha256_hash}"
        if redis_client.client:
            await redis_client.client.delete(exact_key)
            await redis_client.client.delete("rate_limit:test-failover-user")
            
        # Get baseline metrics
        requests_before = get_counter_value(llm_requests_total, status="failover", model=model, provider="anthropic")
        duration_count_before = get_histogram_count(llm_request_duration_seconds, model="gpt-4o", provider="openai")
        prompt_tokens_before = get_counter_value(llm_tokens_total, type="prompt", model=model, provider="anthropic")
        completion_tokens_before = get_counter_value(llm_tokens_total, type="completion", model="gpt-4o", provider="openai")
        cost_before = get_counter_value(llm_cost_total, model="gpt-4o", provider="openai")

        captured_openai_payloads = []

        # Mock failover stream: Anthropic yields some chunks and then errors; OpenAI resumes
        @contextlib.asynccontextmanager
        async def mock_failover_stream(method, url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            
            async def mock_aiter_lines():
                if "api.anthropic.com" in url:
                    yield "event: content_block_delta"
                    yield 'data: {"delta": {"text": "Hello, this is a response from "}}'
                    raise httpx.RemoteProtocolError("Connection closed abruptly")
                elif "api.openai.com" in url:
                    captured_openai_payloads.append(kwargs.get("json"))
                    yield 'data: {"choices": [{"delta": {"content": "Claude which has been completed by OpenAI GPT-4o."}}]}'
                    yield 'data: [DONE]'
                else:
                    raise ValueError(f"Unexpected URL: {url}")
                    
            response.aiter_lines = mock_aiter_lines
            yield response

        # Execute route_and_stream
        received_chunks = []
        with patch.object(httpx.AsyncClient, "stream", side_effect=mock_failover_stream):
            async for chunk in route_and_stream(messages, model, temperature, user_key="test-failover-user"):
                received_chunks.append(chunk)

        # 1. Verify chunks are seamless
        assert len(received_chunks) > 0
        response_text = ""
        for chunk in received_chunks:
            if chunk.startswith("data: "):
                data_str = chunk[len("data: "):].strip()
                if data_str == "[DONE]":
                    break
                data_json = json.loads(data_str)
                response_text += data_json["choices"][0]["delta"]["content"]
                
        expected_full_text = "Hello, this is a response from Claude which has been completed by OpenAI GPT-4o."
        assert response_text == expected_full_text
        assert received_chunks[-1] == "data: [DONE]\n\n"

        # 2. Verify OpenAI continuation payload (System Prompt Override Pattern)
        assert len(captured_openai_payloads) == 1
        openai_payload = captured_openai_payloads[0]
        assert openai_payload["model"] == "gpt-4o"
        assert openai_payload["temperature"] == 0.2
        assert openai_payload["stream"] is True
        
        openai_messages = openai_payload["messages"]
        assert len(openai_messages) == 2
        assert openai_messages[0]["role"] == "system"
        assert "continuation assistant" in openai_messages[0]["content"]
        assert openai_messages[1]["role"] == "user"
        assert "[ORIGINAL USER PROMPT]" in openai_messages[1]["content"]
        assert "[TRUNCATED PARTIAL RESPONSE]" in openai_messages[1]["content"]
        assert "Hello, this is a response from " in openai_messages[1]["content"]

        # 3. Verify Cache matches the complete text
        cached_response = await cache_service.get_cached_response(messages, model, temperature)
        assert cached_response == expected_full_text

        # 4. Verify Prometheus metrics
        requests_after = get_counter_value(llm_requests_total, status="failover", model=model, provider="anthropic")
        duration_count_after = get_histogram_count(llm_request_duration_seconds, model="gpt-4o", provider="openai")
        prompt_tokens_after = get_counter_value(llm_tokens_total, type="prompt", model=model, provider="anthropic")
        completion_tokens_after = get_counter_value(llm_tokens_total, type="completion", model="gpt-4o", provider="openai")
        cost_after = get_counter_value(llm_cost_total, model="gpt-4o", provider="openai")

        assert requests_after == requests_before + 1
        assert duration_count_after == duration_count_before + 1
        assert prompt_tokens_after > prompt_tokens_before
        assert completion_tokens_after > completion_tokens_before
        assert cost_after > cost_before

        # 5. Verify OTel spans
        spans = exporter.get_finished_spans()
        assert len(spans) == 3
        
        anthropic_span = next(s for s in spans if s.name == "anthropic.messages.stream")
        openai_span = next(s for s in spans if s.name == "openai.failover.stream")
        parent_span = next(s for s in spans if s.name == "chat.completions")
        
        assert anthropic_span.status.status_code == StatusCode.ERROR
        assert openai_span.status.status_code == StatusCode.OK
        assert parent_span.status.status_code == StatusCode.OK
        
        assert anthropic_span.attributes["gen_ai.request.model"] == model
        assert openai_span.attributes["gen_ai.request.model"] == "gpt-4o"
        
        assert parent_span.attributes["gen_ai.request.model"] == model
        assert parent_span.attributes["gen_ai.response.model"] == "gpt-4o"
        assert parent_span.attributes["gen_ai.system"] == "anthropic"
        assert parent_span.attributes["gen_ai.usage.completion_tokens"] > 0
        assert parent_span.attributes["gen_ai.usage.prompt_tokens"] > 0
        
        assert anthropic_span.parent is not None
        assert anthropic_span.parent.span_id == parent_span.context.span_id
        assert openai_span.parent is not None
        assert openai_span.parent.span_id == parent_span.context.span_id
    finally:
        await redis_client.disconnect()

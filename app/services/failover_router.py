import time
import json
import logging
from typing import AsyncGenerator, List, Dict, Any, Optional
import httpx
from fastapi import HTTPException
from opentelemetry.trace import Status, StatusCode

from app.core.config import settings
from app.core.redis_client import redis_client
from app.services.token_counter import count_tokens, count_tokens_async
from app.services.cache_service import cache_service
from app.core.observability import (
    get_tracer,
    llm_requests_total,
    llm_request_duration_seconds,
    llm_time_to_first_token_seconds,
    llm_tokens_total,
    llm_cost_total,
)

logger = logging.getLogger(__name__)
tracer = get_tracer()

def openai_to_anthropic_messages(openai_messages: List[Dict[str, Any]]) -> tuple[Optional[str], List[Dict[str, Any]]]:
    """
    Translates OpenAI-style messages list to Anthropic format:
    - Extracts 'system' messages and combines them into a single system prompt string.
    - Ensures roles alternate strictly between 'user' and 'assistant'.
    - Merges consecutive messages of the same role.
    """
    system_parts = []
    anthropic_msgs = []
    
    for msg in openai_messages:
        role = msg.get("role")
        content = msg.get("content")
        if not content:
            continue
            
        if role == "system":
            system_parts.append(content)
        else:
            mapped_role = "user" if role == "user" else "assistant"
            if anthropic_msgs and anthropic_msgs[-1]["role"] == mapped_role:
                anthropic_msgs[-1]["content"] += f"\n\n{content}"
            else:
                anthropic_msgs.append({"role": mapped_role, "content": content})
                
    system_prompt = "\n\n".join(system_parts) if system_parts else None
    return system_prompt, anthropic_msgs

def serialize_messages(messages: List[Dict[str, str]]) -> str:
    """
    Serializes a list of messages into a single text block.
    """
    parts = []
    for msg in messages:
        role = msg.get("role", "")
        content = msg.get("content", "")
        parts.append(f"{role}: {content}")
    return "\n".join(parts)

def estimate_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """
    Estimates the cost of an LLM call in USD based on input and output tokens.
    """
    model_lower = model.lower()
    if "gemini-2.5-flash" in model_lower:
        input_rate = 0.075 / 1_000_000
        output_rate = 0.30 / 1_000_000
    elif "gemini-2.5-pro" in model_lower:
        input_rate = 1.25 / 1_000_000
        output_rate = 5.00 / 1_000_000
    elif "claude-3-5-sonnet" in model_lower:
        input_rate = 3.0 / 1_000_000
        output_rate = 15.0 / 1_000_000
    elif "gpt-4o-mini" in model_lower:
        input_rate = 0.15 / 1_000_000
        output_rate = 0.60 / 1_000_000
    elif "gpt-4o" in model_lower:
        input_rate = 5.0 / 1_000_000
        output_rate = 15.0 / 1_000_000
    else:
        # Default fallback rates (GPT-4o standard rates)
        input_rate = 5.0 / 1_000_000
        output_rate = 15.0 / 1_000_000
        
    return (prompt_tokens * input_rate) + (completion_tokens * output_rate)

def make_openai_chunk(content: str, model: str, finish_reason: Optional[str] = None) -> str:
    """
    Formats a raw text delta as a valid OpenAI SSE streaming chunk.
    """
    chunk = {
        "id": f"chatcmpl-{int(time.time() * 1000)}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": {
                    "content": content
                },
                "logprobs": None,
                "finish_reason": finish_reason
            }
        ]
    }
    return f"data: {json.dumps(chunk)}\n\n"

async def route_and_stream(
    messages: List[Dict[str, Any]],
    model: str,
    temperature: float,
    max_tokens: Optional[int] = None,
    user_key: Optional[str] = None,
    simulate_failover: bool = False,
) -> AsyncGenerator[str, None]:
    """
    Handles routing and streaming of LLM requests with mid-stream failover capability:
    1. Traces the entire process using OpenTelemetry.
    2. Runs pre-flight token bucket checks using Redis rate limiter.
    3. Traces and routes Anthropic calls (for Claude models).
    4. If Anthropic fails mid-stream, recovers and routes to OpenAI using continuation prompt.
    5. Records final metrics, costs, and token deductions.
    """
    start_time = time.time()
    ttft = None
    prompt_tokens = await count_tokens_async(serialize_messages(messages), model)
    completion_tokens = 0
    failover_occurred = False
    final_model = model
    partial_text = ""
    
    # 1. Rate Limiting Check (Pre-charge input tokens)
    bucket_key = f"rate_limit:{user_key or 'global'}"
    if redis_client.client:
        try:
            allowed, remaining, retry_after = await redis_client.execute_rate_limiter(
                key=bucket_key,
                budget_cost=prompt_tokens,
                max_capacity=int(settings.DEFAULT_USER_CAPACITY),
                refill_rate=float(settings.DEFAULT_USER_REFILL_RATE)
            )
            if not allowed:
                logger.warning(f"Rate limit exceeded for {bucket_key}. Cost: {prompt_tokens}, remaining: {remaining}")
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit exceeded. Try again in {retry_after:.2f} seconds."
                )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}. Proceeding as fallback.")

    with tracer.start_as_current_span("chat.completions") as parent_span:
        parent_span.set_attribute("gen_ai.request.model", model)
        parent_span.set_attribute("gen_ai.request.temperature", temperature)
        parent_span.set_attribute("gen_ai.usage.prompt_tokens", prompt_tokens)
        
        is_gemini = "gemini-" in model.lower()
        
        if is_gemini:
            # Route to Google Gemini first
            parent_span.set_attribute("gen_ai.system", "google")
            gemini_span = tracer.start_span("gemini.messages.stream")
            gemini_span.set_attribute("gen_ai.request.model", model)
            
            try:
                # Gemini uses standard OpenAI payload format
                payload = {
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                    "stream": True
                }
                if max_tokens:
                    payload["max_tokens"] = max_tokens
                    
                headers = {
                    "Authorization": f"Bearer {settings.GEMINI_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                logger.info(f"Routing to Google Gemini ({model})...")
                async with httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=15.0, write=None, pool=None)) as client:
                    # Using Google Gemini OpenAI-compatible endpoint
                    async with client.stream("POST", "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions", headers=headers, json=payload) as response:
                        if response.status_code != 200:
                            err_body = await response.aread()
                            raise httpx.HTTPStatusError(
                                f"Gemini API returned {response.status_code}: {err_body.decode()}",
                                request=response.request,
                                response=response
                            )
                            
                        async for line in response.aiter_lines():
                            line = line.strip()
                            if not line:
                                continue
                            if line.startswith("data:"):
                                data_str = line.split("data:", 1)[1].strip()
                                if data_str == "[DONE]":
                                    break
                                data = json.loads(data_str)
                                choices = data.get("choices", [])
                                if choices:
                                    delta_content = choices[0].get("delta", {}).get("content", "")
                                    if delta_content:
                                        partial_text += delta_content
                                        
                                        # TTFT metric
                                        if ttft is None:
                                            ttft = time.time() - start_time
                                            llm_time_to_first_token_seconds.labels(model=model, provider="google").observe(ttft)
                                            gemini_span.set_attribute("gen_ai.time_to_first_token", ttft)
                                            
                                        yield make_openai_chunk(delta_content, model)
                                        
                                        if simulate_failover and len(partial_text) > 30:
                                            logger.warning("Simulating mid-stream failover by raising HTTPX ReadTimeout error...")
                                            raise httpx.ReadTimeout("Simulated Gemini read timeout during generation")
                                    
                gemini_span.set_status(Status(StatusCode.OK))
                gemini_span.end()
                
            except (httpx.RequestError, httpx.HTTPStatusError, Exception) as e:
                logger.error(f"Gemini stream failed mid-stream: {e}. Initiating failover to OpenAI...")
                gemini_span.record_exception(e)
                gemini_span.set_status(Status(StatusCode.ERROR, str(e)))
                gemini_span.end()
                
                failover_occurred = True
        else:
            # Route to OpenAI directly
            parent_span.set_attribute("gen_ai.system", "openai")
            openai_span = tracer.start_span("openai.chat.completions.stream")
            openai_span.set_attribute("gen_ai.request.model", model)
            
            try:
                payload = {
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                    "stream": True
                }
                if max_tokens:
                    payload["max_tokens"] = max_tokens
                    
                headers = {
                    "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                logger.info(f"Routing directly to OpenAI ({model})...")
                async with httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=15.0, write=None, pool=None)) as client:
                    async with client.stream("POST", "https://api.openai.com/v1/chat/completions", headers=headers, json=payload) as response:
                        if response.status_code != 200:
                            err_body = await response.aread()
                            raise httpx.HTTPStatusError(
                                f"OpenAI API returned {response.status_code}: {err_body.decode()}",
                                request=response.request,
                                response=response
                            )
                            
                        async for line in response.aiter_lines():
                            line = line.strip()
                            if not line:
                                continue
                            if line.startswith("data:"):
                                data_str = line.split("data:", 1)[1].strip()
                                if data_str == "[DONE]":
                                    break
                                data = json.loads(data_str)
                                choices = data.get("choices", [])
                                if choices:
                                    delta_content = choices[0].get("delta", {}).get("content", "")
                                    if delta_content:
                                        partial_text += delta_content
                                        
                                        if ttft is None:
                                            ttft = time.time() - start_time
                                            llm_time_to_first_token_seconds.labels(model=model, provider="openai").observe(ttft)
                                            openai_span.set_attribute("gen_ai.time_to_first_token", ttft)
                                            
                                        yield make_openai_chunk(delta_content, model)
                                        
                openai_span.set_status(Status(StatusCode.OK))
                openai_span.end()
            except Exception as e:
                openai_span.record_exception(e)
                openai_span.set_status(Status(StatusCode.ERROR, str(e)))
                openai_span.end()
                raise e

        # 3. Handle OpenAI continuation if failover occurred
        if failover_occurred:
            final_model = "gpt-4o"
            parent_span.set_attribute("gen_ai.response.model", final_model)
            
            openai_failover_span = tracer.start_span("openai.failover.stream")
            openai_failover_span.set_attribute("gen_ai.request.model", final_model)
            
            try:
                # System Prompt Override Pattern
                original_prompt = serialize_messages(messages)
                openai_messages = [
                    {
                        "role": "system",
                        "content": (
                            "You are a continuation assistant. Your ONLY job is to seamlessly continue a response "
                            "that was abruptly cut off mid-sentence. We will supply you with: (1) The Original User Prompt, "
                            "and (2) The Truncated Partial Response generated so far. You must start generating output "
                            "from the EXACT CHARACTER where the Truncated Partial Response leaves off. Do NOT repeat "
                            "any part of the Truncated Partial Response. Do NOT add any introductory transition words, "
                            "conversational filler (e.g., 'Sure, here is the continuation', 'Continuing:'), or wrapping quotes. "
                            "Write ONLY the text necessary to complete the response naturally."
                        )
                    },
                    {
                        "role": "user",
                        "content": f"[ORIGINAL USER PROMPT]\n{original_prompt}\n\n[TRUNCATED PARTIAL RESPONSE]\n{partial_text}"
                    }
                ]
                
                payload = {
                    "model": final_model,
                    "messages": openai_messages,
                    "temperature": 0.2,  # Low temperature for precise completion
                    "stream": True
                }
                
                headers = {
                    "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                logger.info(f"Initiating failover continuation stream via OpenAI ({final_model})...")
                
                # yield a small marker or indicator, or stream immediately.
                # Stitching mechanics: do not insert any spaces, stream the continuation chunks verbatim.
                async with httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=15.0, write=None, pool=None)) as client:
                    async with client.stream("POST", "https://api.openai.com/v1/chat/completions", headers=headers, json=payload) as response:
                        if response.status_code != 200:
                            err_body = await response.aread()
                            raise httpx.HTTPStatusError(
                                f"OpenAI Failover API returned {response.status_code}: {err_body.decode()}",
                                request=response.request,
                                response=response
                            )
                            
                        async for line in response.aiter_lines():
                            line = line.strip()
                            if not line:
                                continue
                            if line.startswith("data:"):
                                data_str = line.split("data:", 1)[1].strip()
                                if data_str == "[DONE]":
                                    break
                                data = json.loads(data_str)
                                choices = data.get("choices", [])
                                if choices:
                                    delta_content = choices[0].get("delta", {}).get("content", "")
                                    if delta_content:
                                        partial_text += delta_content
                                        
                                        # Record TTFT for failover if not recorded yet
                                        if ttft is None:
                                            ttft = time.time() - start_time
                                            llm_time_to_first_token_seconds.labels(model=final_model, provider="openai").observe(ttft)
                                            openai_failover_span.set_attribute("gen_ai.time_to_first_token", ttft)
                                            
                                        yield make_openai_chunk(delta_content, final_model)
                                        
                openai_failover_span.set_status(Status(StatusCode.OK))
                openai_failover_span.end()
                
            except Exception as e:
                logger.error(f"OpenAI failover continuation failed: {e}")
                openai_failover_span.record_exception(e)
                openai_failover_span.set_status(Status(StatusCode.ERROR, str(e)))
                openai_failover_span.end()
                # Yield error chunk
                yield make_openai_chunk(f"\n[Failover service error: {str(e)}]", final_model)

        # 4. End-of-request Metrics & Token Counts
        latency = time.time() - start_time
        completion_tokens = await count_tokens_async(partial_text, final_model)
        total_cost = estimate_cost(final_model, prompt_tokens, completion_tokens)
        
        # Record metrics
        primary_provider = "google" if is_gemini else "openai"
        status_label = "failover" if failover_occurred else "success"
        
        llm_requests_total.labels(status=status_label, model=model, provider=primary_provider).inc()
        llm_request_duration_seconds.labels(model=final_model, provider="openai" if failover_occurred or not is_gemini else "google").observe(latency)
        llm_tokens_total.labels(type="prompt", model=model, provider=primary_provider).inc(prompt_tokens)
        llm_tokens_total.labels(type="completion", model=final_model, provider="openai" if failover_occurred or not is_gemini else "google").inc(completion_tokens)
        llm_cost_total.labels(model=final_model, provider="openai" if failover_occurred or not is_gemini else "google").inc(total_cost)
        
        # Record on parent OTel span
        parent_span.set_attribute("gen_ai.response.model", final_model)
        parent_span.set_attribute("gen_ai.usage.completion_tokens", completion_tokens)
        parent_span.set_attribute("gen_ai.usage.total_tokens", prompt_tokens + completion_tokens)
        parent_span.set_attribute("gen_ai.duration", latency)
        parent_span.set_status(Status(StatusCode.OK))
        
        # 4. Cache the final output if we received content
        if partial_text:
            try:
                await cache_service.set_cached_response(messages, model, temperature, partial_text)
            except Exception as e:
                logger.error(f"Error saving to cache: {e}")
                
        # 5. Rate Limiting Check (Deduct output completion tokens)
        if redis_client.client and completion_tokens > 0:
            try:
                await redis_client.execute_rate_limiter(
                    key=bucket_key,
                    budget_cost=completion_tokens,
                    max_capacity=int(settings.DEFAULT_USER_CAPACITY),
                    refill_rate=float(settings.DEFAULT_USER_REFILL_RATE)
                )
            except Exception as e:
                logger.error(f"Error deducting completion tokens: {e}")
                
        # Yield the final DONE block
        yield "data: [DONE]\n\n"


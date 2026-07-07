import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from app.core.config import settings
from app.core.redis_client import redis_client
from app.services.token_counter import count_tokens, count_tokens_async
from app.services.cache_service import cache_service
from app.services.failover_router import route_and_stream, openai_to_anthropic_messages

@pytest.mark.asyncio
async def test_config():
    assert settings.REDIS_URL == "redis://localhost:6379"
    assert settings.CACHE_SIMILARITY_THRESHOLD == 0.90
    assert settings.DEFAULT_USER_CAPACITY == 10000

@pytest.mark.asyncio
async def test_token_counter():
    text = "Hello world, this is a test prompt."
    # OpenAI count (using tiktoken)
    openai_count = count_tokens(text, "gpt-4o")
    assert openai_count > 0
    
    # Anthropic count (using fallback or tokenizer)
    anthropic_count = count_tokens(text, "claude-3-5-sonnet-20240620")
    assert anthropic_count > 0
    
    # Gemini count (using tiktoken)
    gemini_count = count_tokens(text, "gemini-2.5-flash")
    assert gemini_count > 0
    
    # Async count
    async_count = await count_tokens_async(text, "gpt-4o")
    assert async_count == openai_count

@pytest.mark.asyncio
async def test_redis_client():
    # Make sure redis_client connects, loads script, executes rate limiter
    await redis_client.connect()
    assert redis_client.rate_limiter_sha is not None
    
    key = "test_user_bucket"
    # Clear key first
    if redis_client.client:
        await redis_client.client.delete(key)
        
    # First deduction: should be allowed
    allowed, remaining, retry_after = await redis_client.execute_rate_limiter(
        key=key,
        budget_cost=100,
        max_capacity=1000,
        refill_rate=10
    )
    assert allowed is True
    assert remaining <= 900
    assert retry_after == 0.0
    
    # Large deduction: should not be allowed
    allowed, remaining, retry_after = await redis_client.execute_rate_limiter(
        key=key,
        budget_cost=2000,
        max_capacity=1000,
        refill_rate=10
    )
    assert allowed is False
    assert retry_after == -1.0
    
    await redis_client.disconnect()

@pytest.mark.asyncio
async def test_cache_service():
    await redis_client.connect()
    await cache_service.initialize()
    
    # Recreate the search index for a clean test run
    if cache_service.index:
        try:
            await cache_service.index.clear()
        except Exception:
            pass
            
    messages = [{"role": "user", "content": "What is the speed of light?"}]
    model = "gpt-4o"
    temperature = 0.7
    response = "The speed of light in a vacuum is 299,792,458 meters per second."
    
    # Set cache response
    await cache_service.set_cached_response(messages, model, temperature, response)
    
    # 1. Exact match test
    cached_res = await cache_service.get_cached_response(messages, model, temperature)
    assert cached_res == response
    
    # 2. Semantic match test (different phrasing)
    if cache_service.index is not None:
        similar_messages = [{"role": "user", "content": "Can you tell me the speed of light in a vacuum?"}]
        cached_res_semantic = await cache_service.get_cached_response(similar_messages, model, temperature)
        assert cached_res_semantic == response
    else:
        print("\n[SKIP] Skipping semantic cache test: RediSearch module is not available in local Redis.")
    
    # Clean up index
    if cache_service.index:
        await cache_service.index.disconnect()
    await redis_client.disconnect()

@pytest.mark.asyncio
async def test_openai_to_anthropic_messages():
    openai_messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello!"},
        {"role": "user", "content": "What is 1+1?"},
        {"role": "assistant", "content": "1+1 is 2."},
    ]
    system, messages = openai_to_anthropic_messages(openai_messages)
    assert system == "You are a helpful assistant."
    assert len(messages) == 2
    assert messages[0]["role"] == "user"
    assert "Hello!" in messages[0]["content"]
    assert "What is 1+1?" in messages[0]["content"]
    assert messages[1]["role"] == "assistant"

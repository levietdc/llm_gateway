import time
import json
import logging
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Request, status, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from prometheus_client import make_asgi_app

from app.core.config import settings
from app.core.redis_client import redis_client
from app.services.cache_service import cache_service
from app.services.failover_router import route_and_stream, make_openai_chunk, serialize_messages
from app.services.token_counter import count_tokens
from app.core.observability import init_tracer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Modern Lifespan management for FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    logger.info("Starting up AI Gateway Proxy System...")
    
    # 1. Initialize OpenTelemetry Tracer for Arize Phoenix
    init_tracer("ai-gateway-proxy")
    
    # 2. Connect Redis client and load Lua scripts
    try:
        await redis_client.connect()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.critical(f"Failed to initialize Redis connection: {e}")
        
    # 3. Initialize Semantic Cache (FastEmbed + RedisVL index)
    try:
        await cache_service.initialize()
        logger.info("Initialized Semantic Cache successfully.")
    except Exception as e:
        logger.critical(f"Failed to initialize Semantic Cache: {e}")
        
    yield
    
    # --- Shutdown ---
    logger.info("Shutting down AI Gateway Proxy System...")
    try:
        await redis_client.disconnect()
        logger.info("Disconnected from Redis.")
    except Exception as e:
        logger.error(f"Error during Redis disconnect: {e}")

# Instantiate FastAPI application
app = FastAPI(
    title="AI Gateway Proxy System",
    version="1.0.0",
    lifespan=lifespan
)

# Mount Prometheus metrics application under /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Request validation schemas
class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[Dict[str, Any]]
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    stream: bool = False
    max_tokens: Optional[int] = None

@app.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest, request: Request):
    """
    OpenAI-compatible chat completion proxy endpoint.
    Handles caching, rate limiting, and mid-stream failovers transparently.
    """
    logger.info(f"Received request for model: {req.model}, stream: {req.stream}")
    
    # Extract/derive user key for rate limiting
    user_key = request.headers.get("x-user-key")
    if not user_key:
        auth_header = request.headers.get("authorization")
        if auth_header and auth_header.startswith("Bearer "):
            user_key = auth_header.split(" ")[1]
        else:
            user_key = request.client.host if request.client else "global"
            
    # --- Check Cache ---
    cached_response = None
    try:
        cached_response = await cache_service.get_cached_response(
            messages=req.messages,
            model=req.model,
            temperature=req.temperature
        )
    except Exception as e:
        logger.error(f"Failed to query cache: {e}")
        
    if cached_response:
        logger.info("Cache Hit! Returning cached output.")
        
        if req.stream:
            async def stream_cached_hits():
                yield make_openai_chunk(cached_response, req.model)
                yield "data: [DONE]\n\n"
            return StreamingResponse(stream_cached_hits(), media_type="text/event-stream")
            
        else:
            prompt_tokens = count_tokens(serialize_messages(req.messages), req.model)
            completion_tokens = count_tokens(cached_response, req.model)
            return JSONResponse({
                "id": f"chatcmpl-cache-{int(time.time() * 1000)}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": req.model,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": cached_response
                    },
                    "logprobs": None,
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens
                }
            })
            
    # --- Cache Miss: Route to provider(s) ---
    logger.info("Cache Miss. Routing request to LLM upstream.")
    
    if req.stream:
        # Return Streaming Response yielding SSE events
        return StreamingResponse(
            route_and_stream(
                messages=req.messages,
                model=req.model,
                temperature=req.temperature,
                max_tokens=req.max_tokens,
                user_key=user_key
            ),
            media_type="text/event-stream"
        )
    else:
        # Non-streaming mode: accumulate stream chunks from generator
        full_content = []
        final_model = req.model
        try:
            async for chunk in route_and_stream(
                messages=req.messages,
                model=req.model,
                temperature=req.temperature,
                max_tokens=req.max_tokens,
                user_key=user_key
            ):
                if chunk.startswith("data: "):
                    data_str = chunk[len("data: "):].strip()
                    if data_str == "[DONE]":
                        break
                    try:
                        data_json = json.loads(data_str)
                        choices = data_json.get("choices", [])
                        if choices:
                            content = choices[0].get("delta", {}).get("content", "")
                            full_content.append(content)
                            final_model = data_json.get("model", final_model)
                    except Exception:
                        pass
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error executing non-stream LLM: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Upstream provider failure: {str(e)}"
            )
            
        response_text = "".join(full_content)
        prompt_tokens = count_tokens(serialize_messages(req.messages), req.model)
        completion_tokens = count_tokens(response_text, final_model)
        
        return JSONResponse({
            "id": f"chatcmpl-{int(time.time() * 1000)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": final_model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": response_text
                },
                "logprobs": None,
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens
            }
        })

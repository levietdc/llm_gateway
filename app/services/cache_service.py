import hashlib
import json
import logging
from typing import Optional, Dict, Any, List
import anyio
from fastembed import TextEmbedding
from redisvl.index import AsyncSearchIndex
from redisvl.query import VectorQuery

from app.core.config import settings
from app.core.redis_client import redis_client
from app.core.observability import cache_requests_total

logger = logging.getLogger(__name__)

def serialize_messages(messages: List[Dict[str, str]]) -> str:
    """
    Serializes a list of messages into a single text block for semantic indexing.
    Example output:
      system: You are a helpful assistant.
      user: What is photosynthesis?
    """
    parts = []
    for msg in messages:
        role = msg.get("role", "")
        content = msg.get("content", "")
        parts.append(f"{role}: {content}")
    return "\n".join(parts)

def compute_exact_hash(messages: List[Dict[str, str]], model: str, temperature: float) -> str:
    """
    Computes the SHA-256 hash of a stable JSON serialization of messages, model, and temperature.
    """
    payload = {
        "messages": messages,
        "model": model,
        "temperature": temperature
    }
    payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload_bytes).hexdigest()

class DualLayerCacheService:
    def __init__(
        self,
        redis_url: str = settings.REDIS_URL,
        index_name: str = "llm_semantic_cache",
        model_name: str = settings.DEFAULT_EMBEDDING_MODEL,
        distance_metric: str = "cosine"
    ):
        self.redis_url = redis_url
        self.index_name = index_name
        self.model_name = model_name
        self.distance_metric = distance_metric.lower()
        self.embedding_model = None
        self.index = None
        
        # Determine vector dimensions
        self.vector_dims = 384
        if "all-MiniLM-L6-v2" in model_name or "bge-small" in model_name:
            self.vector_dims = 384
        elif "bge-large" in model_name:
            self.vector_dims = 1024
        elif "nomic-embed" in model_name:
            self.vector_dims = 768

    async def initialize(self):
        """
        Initializes the FastEmbed model and RedisVL AsyncSearchIndex.
        """
        logger.info(f"Initializing FastEmbed model: {self.model_name}...")
        # Run synchronous HuggingFace/FastEmbed loading in a separate thread
        def load_model():
            return TextEmbedding(model_name=self.model_name)
        
        self.embedding_model = await anyio.to_thread.run_sync(load_model)
        
        # Define RedisVL schema
        schema = {
            "index": {
                "name": self.index_name,
                "prefix": f"cache:{self.index_name}",
            },
            "fields": [
                {"name": "prompt", "type": "text"},
                {"name": "response", "type": "text"},
                {
                    "name": "prompt_vector",
                    "type": "vector",
                    "attrs": {
                        "dims": self.vector_dims,
                        "distance_metric": self.distance_metric,
                        "algorithm": "flat",
                        "datatype": "float32"
                    }
                }
            ]
        }
        
        try:
            self.index = AsyncSearchIndex.from_dict(schema, redis_url=self.redis_url)
            exists = await self.index.exists()
            if not exists:
                logger.info(f"Creating semantic cache search index '{self.index_name}' in Redis...")
                await self.index.create(overwrite=False)
            else:
                logger.info(f"Semantic cache search index '{self.index_name}' already exists.")
        except Exception as e:
            logger.warning(
                f"Redis Search (RediSearch) module is not available or failed to initialize: {e}. "
                "Semantic cache (Layer 2) will be disabled. Exact match cache (Layer 1) remains active."
            )
            self.index = None

    async def _get_embedding(self, text: str) -> List[float]:
        """
        Generates embedding vector for the text using FastEmbed (offloaded to thread).
        """
        if not self.embedding_model:
            raise RuntimeError("Cache service is not initialized. Call initialize() first.")
        
        def _embed():
            return list(self.embedding_model.embed([text]))[0].tolist()
            
        return await anyio.to_thread.run_sync(_embed)

    async def get_cached_response(
        self, messages: List[Dict[str, str]], model: str, temperature: float
    ) -> Optional[str]:
        """
        Retrieves a cached response using Exact Match (Layer 1) and Semantic Match (Layer 2).
        If Layer 2 hits, the result is written to Layer 1.
        """
        # --- Layer 1: Exact Match ---
        sha256_hash = compute_exact_hash(messages, model, temperature)
        exact_key = f"cache:exact:{sha256_hash}"
        
        try:
            if redis_client.client:
                cached_res = await redis_client.client.get(exact_key)
                if cached_res:
                    logger.info("Exact Cache Hit (Layer 1)")
                    cache_requests_total.labels(layer="exact", outcome="hit").inc()
                    return cached_res
                else:
                    cache_requests_total.labels(layer="exact", outcome="miss").inc()
        except Exception as e:
            logger.warning(f"Error checking exact cache: {e}")

        # --- Layer 2: Semantic Match ---
        if not self.index:
            logger.warning("Semantic index not initialized, skipping Layer 2 cache check.")
            return None
            
        serialized_prompt = serialize_messages(messages)
        try:
            query_vector = await self._get_embedding(serialized_prompt)
            
            vector_query = VectorQuery(
                vector=query_vector,
                vector_field_name="prompt_vector",
                return_fields=["prompt", "response"],
                num_results=1
            )
            
            results = await self.index.query(vector_query)
            if results:
                best_match = results[0]
                distance = float(best_match.get("vector_distance", 2.0))
                
                # Calculate similarity based on distance metric
                if self.distance_metric == "cosine":
                    similarity = 1.0 - distance
                elif self.distance_metric == "ip":
                    similarity = distance
                else:
                    similarity = 1.0 / (1.0 + distance)
                
                threshold = settings.CACHE_SIMILARITY_THRESHOLD
                logger.info(f"Semantic search: best match similarity = {similarity:.4f} (threshold: {threshold:.2f})")
                
                if similarity >= threshold:
                    logger.info("Semantic Cache Hit (Layer 2)")
                    cache_requests_total.labels(layer="semantic", outcome="hit").inc()
                    response = best_match.get("response")
                    
                    # Promote to Layer 1 for O(1) next time
                    if redis_client.client and response:
                        await redis_client.client.set(exact_key, response, ex=86400) # 24h expiration
                        
                    return response
                else:
                    cache_requests_total.labels(layer="semantic", outcome="miss").inc()
            else:
                cache_requests_total.labels(layer="semantic", outcome="miss").inc()
        except Exception as e:
            logger.error(f"Error checking semantic cache: {e}")
            
        return None

    async def set_cached_response(
        self, messages: List[Dict[str, str]], model: str, temperature: float, response: str
    ) -> None:
        """
        Saves the response into both Layer 1 and Layer 2 caches.
        """
        if not response:
            return

        # --- Set Layer 1: Exact Match ---
        sha256_hash = compute_exact_hash(messages, model, temperature)
        exact_key = f"cache:exact:{sha256_hash}"
        try:
            if redis_client.client:
                await redis_client.client.set(exact_key, response, ex=86400) # 24h expiration
                logger.info(f"Cached in Layer 1. Key: {exact_key}")
        except Exception as e:
            logger.warning(f"Failed to set exact cache: {e}")

        # --- Set Layer 2: Semantic Match ---
        if not self.index:
            logger.warning("Semantic index not initialized, skipping Layer 2 cache set.")
            return
            
        serialized_prompt = serialize_messages(messages)
        try:
            vector = await self._get_embedding(serialized_prompt)
            doc = {
                "prompt": serialized_prompt,
                "response": response,
                "prompt_vector": vector
            }
            # load expects an iterable of documents
            await self.index.load([doc], ttl=86400) # 24h TTL for semantic cache as well
            logger.info("Cached in Layer 2 (Semantic Cache)")
        except Exception as e:
            logger.error(f"Failed to set semantic cache: {e}")

cache_service = DualLayerCacheService()

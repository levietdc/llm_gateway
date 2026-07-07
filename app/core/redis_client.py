import os
import logging
import redis.asyncio as aioredis
from app.core.config import settings

logger = logging.getLogger(__name__)

class RedisClientManager:
    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis_url = redis_url
        self.pool: aioredis.ConnectionPool | None = None
        self.client: aioredis.Redis | None = None
        self.rate_limiter_sha: str | None = None

    async def connect(self):
        if not self.pool:
            logger.info(f"Connecting to Redis at {self.redis_url}...")
            self.pool = aioredis.ConnectionPool.from_url(
                self.redis_url,
                decode_responses=True,
                max_connections=100
            )
            self.client = aioredis.Redis(connection_pool=self.pool)
            
            # Load Lua rate limiter script
            await self._load_lua_script()

    async def _load_lua_script(self):
        lua_path = "/Users/leviet/.gemini/antigravity/worktrees/llm_gateway/ai-gateway-proxy-system/docs/research/rate_limiter.lua"
        if not os.path.exists(lua_path):
            logger.error(f"Lua rate limiter script not found at: {lua_path}")
            raise FileNotFoundError(f"Lua script not found at {lua_path}")
        
        with open(lua_path, "r", encoding="utf-8") as f:
            lua_script = f.read()
        
        self.rate_limiter_sha = await self.client.script_load(lua_script)
        logger.info(f"Loaded Lua rate limiter script. SHA: {self.rate_limiter_sha}")

    async def disconnect(self):
        logger.info("Disconnecting from Redis...")
        if self.client:
            await self.client.aclose()
        if self.pool:
            await self.pool.disconnect()
        self.client = None
        self.pool = None
        self.rate_limiter_sha = None

    async def execute_rate_limiter(
        self, key: str, budget_cost: int, max_capacity: int, refill_rate: float
    ) -> tuple[bool, float, float]:
        """
        Executes the rate limiter Lua script.
        
        Args:
            key: Redis key for the user's rate limit bucket.
            budget_cost: Number of tokens requested.
            max_capacity: Max capacity of the bucket.
            refill_rate: Token refill rate per second.
            
        Returns:
            Tuple of (allowed: bool, remaining_tokens: float, retry_after: float)
        """
        if not self.client or not self.rate_limiter_sha:
            raise RuntimeError("Redis client is not connected or Lua script is not loaded.")
        
        # Execute the Lua script using evalsha
        result = await self.client.evalsha(
            self.rate_limiter_sha,
            1,  # Number of keys
            key,
            budget_cost,
            max_capacity,
            refill_rate
        )
        
        # Result is list: [allowed, remaining_tokens, retry_after]
        allowed = bool(result[0])
        remaining_tokens = float(result[1]) if result[1] is not None else 0.0
        retry_after = float(result[2]) if result[2] is not None else 0.0
        
        return allowed, remaining_tokens, retry_after

redis_client = RedisClientManager()

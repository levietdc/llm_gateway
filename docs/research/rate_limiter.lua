-- docs/research/rate_limiter.lua
-- Atomically deducts tokens from a token bucket rate limiter key in Redis.
-- Supports automatic refills over time.
--
-- KEYS[1]: The rate limiter bucket key (Redis Hash)
-- ARGV[1]: budget_cost    (number of tokens to deduct)
-- ARGV[2]: max_capacity   (maximum tokens the bucket can hold)
-- ARGV[3]: refill_rate    (number of tokens refilled per second)
--
-- Returns an array:
-- [1] allowed          (1 if tokens were deducted, 0 otherwise)
-- [2] remaining_tokens (remaining tokens in the bucket)
-- [3] retry_after      (seconds to wait before enough tokens are available for this cost; -1 if it can never be satisfied)

local rate_limit_key = KEYS[1]
local budget_cost = tonumber(ARGV[1])
local max_capacity = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])

-- Get current time from Redis server (avoids client clock drift)
local redis_time = redis.call('time')
local now = tonumber(redis_time[1]) + (tonumber(redis_time[2]) / 1000000)

-- Check input parameters
if not budget_cost or not max_capacity or not refill_rate or budget_cost <= 0 or max_capacity <= 0 or refill_rate <= 0 then
    return {0, 0, -1}
end

-- Read current state from Redis
local data = redis.call('HMGET', rate_limit_key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens or not last_updated then
    -- Initialize the bucket to max capacity
    tokens = max_capacity
    last_updated = now
else
    -- Calculate refilled tokens based on elapsed time
    local elapsed = now - last_updated
    if elapsed > 0 then
        local refill = elapsed * refill_rate
        tokens = math.min(max_capacity, tokens + refill)
        last_updated = now
    end
end

-- Calculate if request can be accommodated
local allowed = 0
local retry_after = 0

if budget_cost > max_capacity then
    -- Request exceeds maximum capacity of the bucket, can never be satisfied
    allowed = 0
    retry_after = -1
elseif tokens >= budget_cost then
    -- Deduct tokens
    tokens = tokens - budget_cost
    allowed = 1
    retry_after = 0
else
    -- Insufficient tokens
    allowed = 0
    local deficit = budget_cost - tokens
    retry_after = deficit / refill_rate
end

-- Save the updated state to Redis
redis.call('HMSET', rate_limit_key, 'tokens', tokens, 'last_updated', last_updated)

-- Calculate dynamic TTL (time until bucket is fully refilled + 60s buffer)
-- This prevents keys from leaking while keeping them alive during active usage
local time_to_full = math.ceil((max_capacity - tokens) / refill_rate)
local ttl = math.max(60, time_to_full + 60)
redis.call('EXPIRE', rate_limit_key, ttl)

return {allowed, tokens, retry_after}

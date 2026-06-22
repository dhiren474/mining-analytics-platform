# api/cache.py
import os, json
from typing import Any, Optional
from redis.asyncio import Redis
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

redis_client = Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

async def get_cached(key: str) -> Optional[Any]:
    try:
        value = await redis_client.get(key)
        return json.loads(value) if value else None
    except Exception as e:
        logger.warning(f"Redis GET failed for {key}: {e}")
        return None

async def set_cached(key: str, value: Any, ttl: int = 30):
    try:
        await redis_client.setex(key, ttl, json.dumps(value, default=str))
    except Exception as e:
        logger.warning(f"Redis SET failed for {key}: {e}")

def cache(ttl: int = 30):
    """Decorator to cache endpoint responses in Redis."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Build cache key from function name + kwargs
            cache_key = f"{func.__name__}:{json.dumps(kwargs, sort_keys=True, default=str)}"
            cached = await get_cached(cache_key)
            if cached:
                logger.debug(f"Cache HIT: {cache_key}")
                return cached
            result = await func(*args, **kwargs)
            await set_cached(cache_key, result, ttl)
            return result
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator

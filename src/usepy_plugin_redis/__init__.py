from .store import RedisStore
from .stream import RedisStreamStore

useRedis = RedisStore
useRedisStreamStore = RedisStreamStore

__all__ = [
    "useRedis",
    "useRedisStreamStore"
]

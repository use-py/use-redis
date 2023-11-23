from .store import RedisStore as useRedis
from .stream import RedisStreamStore as useRedisStream, RedisStreamMessage

useRedisStreamStore = useRedisStream
useRedisStore = useRedis

__all__ = [
    "useRedis",
    "useRedisStore",
    "useRedisStreamStore",
    "RedisStreamMessage",
    "useRedisStream",
]

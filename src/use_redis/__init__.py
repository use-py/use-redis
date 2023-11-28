from .store import RedisStore as useRedis
from .stream import RedisStreamMessage
from .stream import RedisStreamStore as useRedisStream

useRedisStreamStore = useRedisStream
useRedisStore = useRedis

__all__ = [
    "useRedis",
    "useRedisStore",
    "useRedisStreamStore",
    "RedisStreamMessage",
    "useRedisStream",
]

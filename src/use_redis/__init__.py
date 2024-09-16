from .store import RedisStore as useRedis
from .stream import RedisStreamMessage
from .stream import RedisStreamStore as useRedisStream
from .set import RedisSetStore as useRedisSet

useRedisStreamStore = useRedisStream
useRedisStore = useRedis

__all__ = [
    "useRedis",
    "useRedisStore",
    "useRedisSet",
    "useRedisStreamStore",
    "RedisStreamMessage",
    "useRedisStream",
]

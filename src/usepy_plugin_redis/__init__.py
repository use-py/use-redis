from .store import RedisStore as useRedis
from .stream import RedisStreamStore as useRedisStream
from .lock import Lock as useRedisLock

useRedisStreamStore = useRedisStream

__all__ = [
    "useRedis",
    "useRedisStreamStore",
    "useRedisStream",
    "useRedisLock"
]

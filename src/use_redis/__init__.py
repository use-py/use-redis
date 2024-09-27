from .store import RedisStore as useRedis
from .stream import RedisStreamMessage
from .stream import RedisStreamStore as useRedisStream
from .set import RedisSetStore as useRedisSet
from .list import RedisListStore as useRedisList
from .hash import RedisHashStore as useRedisHash

useRedisStreamStore = useRedisStream
useRedisStore = useRedis

__all__ = [
    "useRedis",
    "useRedisStore",
    "useRedisSet",
    "useRedisStreamStore",
    "RedisStreamMessage",
    "useRedisStream",
    "useRedisList",
    "useRedisHash",
]

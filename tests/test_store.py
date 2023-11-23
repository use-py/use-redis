import pytest
import redis
from redis.exceptions import ConnectionError

from usepy_plugin_redis.store import RedisStore


@pytest.fixture
def redis_store():
    store = RedisStore()
    yield store
    store.shutdown()


def test_create_connection(redis_store):
    connection = redis_store.connection
    assert connection is not None


def test_shutdown(redis_store):
    connection = redis_store.connection
    assert connection is not None
    redis_store.shutdown()
    assert redis_store._connection is None


def test_redis_connection():
    rds = RedisStore(host='localhost_', port=6379, db=0)
    rds.MAX_CONNECTION_ATTEMPTS = 1
    with pytest.raises(ConnectionError):
        rds.connection

import pytest

from usepy_plugin_redis import useRedisStreamStore


@pytest.fixture
def redis():
    return useRedisStreamStore()


def test_send(redis):
    stream = 'test_stream'
    message = {'foo': 'bar'}
    redis.send(stream, message)
    assert redis.connection.xlen(stream) == 1

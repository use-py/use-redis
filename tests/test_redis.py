import threading

import pytest

from usepy_plugin_redis import useRedisStreamStore


@pytest.fixture
def redis():
    return useRedisStreamStore(stream='test_stream')


def test_connection(redis):
    """
    Test that the connection is established
    """
    assert redis.connection.ping()


def test_send(redis):
    stream = 'test_stream'
    message = {'foo': 'bar'}
    redis.send(stream, message)
    assert redis.connection.xlen(stream) >= 1


def test_consumer(redis):
    stream = 'test_consumer_stream'
    send_message = {'foo': 'bar'}
    redis.send(stream, send_message)

    def callback(message):
        assert isinstance(message, list)
        assert len(message) == 1
        first_message = message[0]
        message_id, message = first_message
        _message = {
            k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
            for k, v in message.items()
        }
        assert _message == send_message
        redis.shutdown()

    threading.Thread(target=redis.start_consuming, args=(stream, 'test_group', 'test_consumer', callback)).start()

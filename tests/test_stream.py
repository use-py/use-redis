import threading

import pytest

from usepy_plugin_redis import useRedisStreamStore


@pytest.fixture
def redis():
    return useRedisStreamStore(stream_name='test_stream')


def test_connection(redis):
    """
    Test that the connection is established
    """
    assert redis.connection.ping()


def test_send(redis):
    message = {'foo': 'bar'}
    redis.send(message)
    assert redis.connection.xlen('test_stream') >= 1


def test_consumer(redis):
    stream = 'test_consumer_stream'
    redis.stream_name = stream
    send_message = {'foo': 'bar'}
    redis.send(send_message)

    def callback(message, consumer_group, *args, **kwargs):
        assert isinstance(message, list)
        assert len(message) == 1
        first_message = message[0]
        message_id, message = first_message
        print("message_id", message_id)
        _message = {
            k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
            for k, v in message.items()
        }
        assert _message == send_message
        redis.connection.xack(stream, consumer_group, message_id)
        redis.shutdown()

    threading.Thread(target=redis.start_consuming, args=("consumer_name", "consumer_group1", callback,)).start()
    threading.Thread(target=redis.start_consuming, args=("consumer_name", "consumer_group2", callback,)).start()
    threading.Thread(target=redis.start_consuming, args=("consumer_name", "consumer_group3", callback,)).start()

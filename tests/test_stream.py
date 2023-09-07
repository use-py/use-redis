import threading
import time

import pytest

from usepy_plugin_redis import useRedisStreamStore, RedisStreamMessage


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
    
    def callback(message: RedisStreamMessage, *args, **kwargs):
        # print("message", message.to_dict())
        assert message.body == send_message
        redis.connection.xack(message.stream_name, message.consumer_group, message.message_id)
        redis.shutdown()
    
    # threading.Thread(target=redis.start_consuming, args=("consumer_group1", "consumer_name", callback,)).start()
    threading.Thread(target=redis.start_consuming, args=("consumer_group2", "consumer_name", callback,),
                     kwargs={'prefetch': 2}).start()
    # threading.Thread(target=redis.start_consuming, args=("consumer_group3", "consumer_name", callback,)).start()


def test_consumer_xclaim(redis):
    stream = 'test_consumer_xclaim_stream'
    redis.connection.delete(stream)
    
    redis.stream_name = stream
    send_message = {'foo': 'bar'}
    redis.send(send_message)
    
    def callback(message: RedisStreamMessage, *args, **kwargs):
        # print("message", message.to_dict())
        assert message.body == send_message
        redis.connection.xack(message.stream_name, message.consumer_group, message.message_id)
        redis.shutdown()
    
    redis._create_group("consumer_group")
    redis.consume("consumer_group", "consumer1", prefetch=1, claim_min_idle_time=0, force_claim=True)
    time.sleep(1)
    job = threading.Thread(target=redis.start_consuming,
                           args=("consumer_group", "consumer2", callback,),
                           kwargs={'prefetch': 1, 'xclaim_interval': 5000, 'timeout': 500, 'force_claim': True}
                           )
    job.start()
    job.join()
    
    

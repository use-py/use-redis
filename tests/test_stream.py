import json
import time

import pytest
import redis

from use_redis.stream import RedisStreamMessage, RedisStreamStore


# 测试RedisStreamMessage
class TestRedisStreamMessage:
    def test_to_dict(self):
        message = RedisStreamMessage("1", {"foo": "bar"}, "test_stream", "test_group")
        message_dict = message.to_dict()
        assert isinstance(message_dict, dict)
        assert message_dict["id"] == "1"
        assert message_dict["body"] == {"foo": "bar"}

    def test_to_json(self):
        message = RedisStreamMessage("1", {"foo": "bar"}, "test_stream", "test_group")
        message_json = message.to_json()
        assert isinstance(message_json, str)
        assert json.loads(message_json) == {
            "id": "1",
            "body": {"foo": "bar"},
            "stream": "test_stream",
            "group": "test_group",
        }


# 创建一个测试Redis连接
def test_redis_connection():
    connection = redis.StrictRedis(host="localhost", port=6379, db=0)
    connection.flushall()  # 清空测试数据


# 测试RedisStreamStore
class TestRedisStreamStore:
    @pytest.fixture
    def redis_stream_store(self):
        store = RedisStreamStore(stream="test_stream", group="test_group")
        yield store

    def test_claim_old_pending_messages(self, redis_stream_store):
        consumer = "test_consumer"
        message = {"foo": "bar"}
        redis_stream_store.send(message)
        redis_stream_store.get(consumer, 1)
        messages = redis_stream_store.claim_old_pending_messages(
            consumer, count=1, min_idle_time=0
        )
        assert isinstance(messages, list)
        assert len(messages) == 1
        assert isinstance(messages[0], RedisStreamMessage)
        assert messages[0].body == message
        redis_stream_store.ack(messages[0])

    def test_get_messages(self, redis_stream_store):
        consumer = "test_consumer"
        message = {"foo": "bar"}
        redis_stream_store.send(message)
        messages = redis_stream_store.get(consumer, count=1)
        assert isinstance(messages, list)
        assert len(messages) == 1
        assert isinstance(messages[0], RedisStreamMessage)
        assert messages[0].body == message
        redis_stream_store.ack(messages[0])

    def test_consume_messages(self, redis_stream_store):
        consumer = "test_consumer"
        message = {"foo": "bar"}
        redis_stream_store.send(message)
        messages = redis_stream_store.consume(consumer, prefetch=1, timeout=1000)
        assert isinstance(messages, list)
        assert len(messages) == 1
        assert isinstance(messages[0], RedisStreamMessage)
        assert messages[0].body == message
        redis_stream_store.ack(messages[0])

    def test_consume_messages_with_idle_time(self, redis_stream_store):
        consumer = "test_consumer"
        message = {"foo": "bar"}
        redis_stream_store.send(message)
        redis_stream_store.get(consumer + "_get", 1)
        messages = redis_stream_store.consume(
            consumer, prefetch=1, force_claim=True, redeliver_timeout=500
        )
        assert messages == []
        time.sleep(1)
        messages = redis_stream_store.consume(
            consumer, prefetch=1, force_claim=True, redeliver_timeout=500
        )
        assert isinstance(messages, list)
        assert len(messages) == 1
        assert isinstance(messages[0], RedisStreamMessage)
        assert messages[0].body == message
        redis_stream_store.ack(messages[0])

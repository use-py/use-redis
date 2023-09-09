import json
import logging
import threading
import time
from typing import Optional, List, Union, Callable

import redis

from .store import RedisStore

logger = logging.getLogger(__name__)


class RedisStreamMessage:
    """
    A message from the Redis stream.
    """
    
    def __init__(self, id, body, stream, group):
        self.stream = stream
        self.group = group
        self.id = id
        self.body = body
    
    @staticmethod
    def from_xread(raw_messages, *, group):
        """
        Parse a raw message from the Redis stream xread[group] respond.
        """
        # xread = [['test_consumer_stream', [('1693561850564-0', {'foo': 'bar'}), ('1693561905479-0', {'foo': 'bar'})]]]
        
        if not group:
            raise ValueError("group is required")
        
        result = []
        for stream, messages in raw_messages:
            result.extend(RedisStreamMessage(id, body, stream, group) for id, body in messages)
        
        return result
    
    @staticmethod
    def from_xclaim(raw_messages, *, stream, group):
        """
        Parse a raw message from the Redis stream xclaim respond.
        """
        # xclaim = [('1693561850564-0', {'foo': 'bar'}), ('1693561905479-0', {'foo': 'bar'})]
        
        if not stream:
            raise ValueError("stream is required")
        if not group:
            raise ValueError("group is required")
        
        return [RedisStreamMessage(id, body, stream, group) for id, body in raw_messages]
    
    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    
    def to_json(self):
        return json.dumps(self.to_dict())
    
    def __str__(self):
        return self.to_json()
    
    def __repr__(self):
        return f"RedisStreamMessage({self.stream}, {self.group}, {self.id}, {self.body})"


class RedisStreamStore(RedisStore):
    
    def __init__(
            self,
            *,
            stream: str,
            group: str,
            stream_max_entries: int = 0,
            redeliver_timeout: int = 60000,
            claim_interval: int = 1800000,
            **kwargs
    ):
        """
        
        A Redis stream store.
        
        :param stream: The name of the stream.
        :param group: The name of the group.
        :param stream_max_entries: any value higher than 0 defines an approximate maximum number of stream entries
        :param redeliver_timeout: Timeout before redeliver messages still in pending state (seconds)
        :param claim_interval: Interval by which pending/abandoned messages should be checked

        """
        super().__init__(**kwargs)
        self.stream = stream
        self.group = group
        self.max_entries = stream_max_entries if stream_max_entries > 0 else None
        self.redeliver_timeout = redeliver_timeout
        self.claim_interval = claim_interval
        
        self._auto_setup = True
        self.state = threading.local()
    
    def _setup(self):
        try:
            self.connection.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "already exists" not in str(e):
                raise e
        
        self._auto_setup = False
    
    def send(self, message: dict):
        """
        Send a message to the Redis stream.
        """
        if self._auto_setup:
            self._setup()
        return self.connection.xadd(self.stream, message, maxlen=self.max_entries)
    
    def claim_old_pending_messages(
            self, consumer: str,
            count: int,
            min_idle_time: int
    ) -> Optional[List[RedisStreamMessage]]:
        """
        Claim messages from the Redis stream.
        """
        # https://redis.io/commands/xautoclaim/
        # 在迭代 PEL 时，如果偶然发现流中不再存在的消息（由 修剪 XDEL 或删除），它不会声明它 XAUTOCLAIM ，而是将其从找到它的 PEL 中删除。
        # 此功能在 Redis 7.0 中引入。这些消息 ID 作为回复的一部分 XAUTOCLAIM 返回给调用方。
        _, messages, _ = self.connection.xautoclaim(
            name=self.stream,
            groupname=self.group,
            consumername=consumer,
            min_idle_time=min_idle_time,
            start_id="0",
            count=count
        )
        logger.debug(f"xautoclaim: {messages=}")
        
        self.state.next_claim = time.time() * 1000 + self.claim_interval
        
        if messages:
            return RedisStreamMessage.from_xclaim(raw_messages=messages, stream=self.stream, group=self.group)
    
    def get(self, consumer: str, count: int = 1, block: Union[int, None] = None) -> Optional[List[RedisStreamMessage]]:
        """
        Read new messages from the Redis stream without claim.
        """
        if self._auto_setup:
            self._setup()
        
        raw_messages = self.connection.xreadgroup(
            groupname=self.group,
            consumername=consumer,
            streams={self.stream: ">"},
            count=count,
            block=block
        )
        logger.debug(f"xreadgroup: {raw_messages=}")
        if raw_messages:
            return RedisStreamMessage.from_xread(raw_messages=raw_messages, group=self.group)
    
    def consume(
            self,
            consumer: str,
            prefetch: int = 1,
            timeout: Union[int, None] = None,
            force_claim: bool = False,
            redeliver_timeout: Union[int, None] = None
    ) -> Optional[List[RedisStreamMessage]]:
        
        """
        Consume messages from the Redis stream(order: xclaim -> xreadgroup).
        
        :param consumer: The name of the consumer
        :param prefetch: Number of prefetches
        :param timeout: Blocking time of the Xread. Unit is millisecond, 0 is infinite blocking
        :param force_claim: Whether to force claim, True= to perform claim, False = execute claim periodically
        :param redeliver_timeout: Timeout before redeliver messages still in pending state (seconds)
        :return: List of messages consumed. If consumption fails, return []
        """
        
        # 获取 当前消费者尚未ACK 的消息，最多获取  prefetch 个
        pending_messages = self.connection.xpending_range(self.stream, self.group, '-', '+', prefetch, consumer, 0)
        # 计算需获取的消息数量
        need_count = prefetch - len(pending_messages) if pending_messages else prefetch
        if need_count <= 0:
            return []
        
        result = []
        # 先尝试 xclaim
        if force_claim or getattr(self.state, "next_claim", 0) <= time.time() * 1000:
            pending_messages = self.claim_old_pending_messages(
                consumer=consumer,
                count=need_count,
                min_idle_time=redeliver_timeout or self.redeliver_timeout
            )
            if pending_messages:
                result.extend(pending_messages)
                need_count = need_count - len(result)  # 更新还需获取的消息数量
        
        # 然后 xreadgroup
        if need_count > 0:
            messages = self.get(consumer, need_count, timeout)
            if messages:
                result.extend(messages)
        
        return result
    
    def start_consuming(self, consumer: str, callback: Callable, prefetch: int = 1, timeout: int = 1000, **kwargs):
        """
        Start consuming messages from the Redis stream.
        
        :param consumer: The name of the consumer，please use a unique value
        :param callback: Callback function
        :param prefetch: Number of prefetches
        :param timeout: Blocking time of the Xread. Unit is millisecond, 0 is infinite blocking
        """
        
        while not self._shutdown:
            try:
                messages = self.consume(consumer, prefetch, timeout=timeout, **kwargs)
                for message in messages:
                    callback(message)
            except redis.RedisError as e:
                logger.error(f"Error consuming messages: {e}")
                time.sleep(self.RECONNECTION_DELAY)
    
    def ack(self, message: RedisStreamMessage):
        """
        Acknowledge a message.
        """
        self.connection.xack(self.stream, self.group, message.id)
    
    def reject(self, message: RedisStreamMessage):
        """
        Reject a message.
        """
        self.connection.xack(self.stream, self.group, message.id)
        

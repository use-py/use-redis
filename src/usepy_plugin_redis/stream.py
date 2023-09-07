import json
import logging
import threading
import time
from typing import Optional, List

import redis

from .store import RedisStore

logger = logging.getLogger(__name__)


class RedisStreamMessage:
    """
    A message from the Redis stream.
    """
    
    def __init__(self, message_id, message_body, stream_name, consumer_group, consumer_name: Optional[str] = None):
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.message_id = message_id
        self.body = message_body
    
    @staticmethod
    def from_xread(raw_messages, *, consumer_group, consumer_name: Optional[str] = None):
        """
        Parse a raw message from the Redis stream xread[group] respond.
        """
        # xread = [['test_consumer_stream', [('1693561850564-0', {'foo': 'bar'}), ('1693561905479-0', {'foo': 'bar'})]]]
        
        if not consumer_group:
            raise ValueError("consumer_group is required")
        
        result = []
        for stream_name, messages in raw_messages:
            result.extend(
                RedisStreamMessage(message_id, message_body, stream_name, consumer_group, consumer_name)
                for message_id, message_body in messages
            )
        
        return result
    
    @staticmethod
    def from_xclaim(raw_messages, *, stream_name, consumer_group, consumer_name: Optional[str] = None):
        """
        Parse a raw message from the Redis stream xclaim respond.
        """
        # xclaim = [('1693561850564-0', {'foo': 'bar'}), ('1693561905479-0', {'foo': 'bar'})]
        
        if not stream_name:
            raise ValueError("stream_name is required")
        if not consumer_group:
            raise ValueError("consumer_group is required")
        
        return [RedisStreamMessage(message_id, message_body, stream_name, consumer_group, consumer_name)
                for message_id, message_body in raw_messages]
    
    def to_dict(self):
        return {
            "stream_name": self.stream_name,
            "consumer_group": self.consumer_group,
            "consumer_name": self.consumer_name,
            "message_id": self.message_id,
            "message_body": self.body
        }
    
    def to_json(self):
        return json.dumps(self.to_dict())
    
    def __str__(self):
        return self.to_json()
    
    def __repr__(self):
        return f"RedisStreamMessage({self.stream_name}, {self.consumer_group}, {self.message_id}, {self.body})"


class RedisStreamStore(RedisStore):
    
    def __init__(self, stream_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_name = stream_name
        self.state = threading.local()
    
    def send(self, message: dict):
        """
        Send a message to the Redis stream.
        """
        try:
            return self.connection.xadd(self.stream_name, message)
        except redis.RedisError as e:
            logger.error(f"Failed to send message: {e}")
            raise
    
    def _create_group(self, consumer_group, start_id="0-0"):
        try:
            self.connection.xgroup_create(self.stream_name, consumer_group, id=start_id, mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "already exists" not in str(e):
                raise e
    
    def _is_need_xclaim(self):
        """判断是否需要 xclaim"""
        return time.time() * 1000 - self.state.xclaim_last_time > self.state.xclaim_interval
    
    def _xautoclaim(
            self,
            consumer_name: str,
            consumer_group: str,
            count: int,
            min_idle_time: int
    ) -> Optional[List[RedisStreamMessage]]:
        """
        Claim messages from the Redis stream.
        """
        # TODO: 异常需要重试
        try:
            _, pending_messages, _ = self.connection.xautoclaim(
                self.stream_name, consumer_group, consumer_name,
                min_idle_time=min_idle_time,
                start_id="0-0",
                count=count
            )
            logger.debug(f"xautoclaim: {pending_messages=}")
            if pending_messages:
                return RedisStreamMessage.from_xclaim(
                    raw_messages=pending_messages,
                    stream_name=self.stream_name,
                    consumer_group=consumer_group,
                    consumer_name=consumer_name
                )
        except redis.RedisError as e:
            logger.error(f"Error claiming messages: {e}")
            time.sleep(self.RECONNECTION_DELAY)
    
    def _xreadgroup(
            self,
            consumer_name: str,
            consumer_group: str,
            count: int,
            block: Optional[int] = None
    ) -> Optional[List[RedisStreamMessage]]:
        """
        Read messages from the Redis stream.
        """
        try:
            raw_messages = self.connection.xreadgroup(
                groupname=consumer_group,
                consumername=consumer_name,
                streams={self.stream_name: ">"},
                count=count,
                block=block,
            )
            logger.debug(f"xreadgroup: {raw_messages=}")
            if raw_messages:
                return RedisStreamMessage.from_xread(
                    raw_messages=raw_messages,
                    consumer_group=consumer_group,
                    consumer_name=consumer_name,
                )
        except redis.RedisError as e:
            logger.error(f"Error reading messages: {e}")
            time.sleep(self.RECONNECTION_DELAY)
    
    def consume(
            self,
            consumer_group: str,
            consumer_name: str,
            prefetch: int,
            claim_min_idle_time: int = 3600000,
            force_claim: bool = False,
            block: Optional[int] = None
    ) -> Optional[List[RedisStreamMessage]]:
        """
        Consume messages from the Redis stream(order: xclaim -> xreadgroup).
        
        :param consumer_name: 消费者名称
        :param consumer_group: 消费组名称
        :param prefetch: 消费数量
        :param block: xreadgroup 阻塞时间，单位毫秒，默认为 None，不阻塞
        :param claim_min_idle_time: xclaim 最小空闲的时间（即消息消费的超时时间），单位毫秒，默认为 1 小时
        :param force_claim: 是否强制 xclaim，True=忽略上次 xclaim 的时间和 xclaim 间隔限制，False=按周期执行 xclaim
        
        :return: 消费的消息列表，消费失败时则返回 []
        """
        # 获取 当前消费者尚未ACK 的消息，最多获取  prefetch 个
        pending_messages = self.connection.xpending_range(
            name=self.stream_name,
            groupname=consumer_group,
            min='-',
            max='+',
            count=prefetch,
            idle=0,
            consumername=consumer_name,
        )
        # 计算需获取的消息数量
        need_count = prefetch - len(pending_messages) if pending_messages else prefetch
        if need_count <= 0:
            return []
        
        result = []
        # 先尝试 xclaim
        if force_claim or self._is_need_xclaim():
            pending_messages = self._xautoclaim(consumer_name, consumer_group, need_count, claim_min_idle_time)
            if pending_messages:
                result.extend(pending_messages)
                # 更新还需获取的消息数量
                need_count = need_count - len(result)
            
            self.state.xclaim_last_time = time.time() * 1000
        
        # 然后 xreadgroup
        if need_count > 0:
            messages = self._xreadgroup(consumer_name, consumer_group, need_count, block)
            if messages:
                result.extend(messages)
        
        return result
    
    def start_consuming(self, consumer_group, consumer_name, callback, prefetch=1, timeout=3600000, **kwargs):
        """
        Start consuming messages from the Redis stream.
        
        :param consumer_name: 消费者名称
        :param consumer_group: 消费组名称
        :param callback: 消费回调函数
        :param prefetch: 消费并发数量
        :param timeout: 消费超时时间(即 xclaim 最小空闲的时间)，默认为 1 小时
        """
        # 初始化工作
        self._create_group(consumer_group, kwargs.get('group_start_id', '0-0'))
        block = kwargs.get('xread_block', None)
        
        # xclaim 时必要的参数，得线程隔离
        self.state.xclaim_interval = kwargs.get('xclaim_interval', 5 * 60 * 1000)  # xclaim 间隔时间，单位毫秒，默认 5 分钟
        self.state.xclaim_last_time = 0
        
        while not self._shutdown:
            try:
                messages = self.consume(consumer_group, consumer_name, prefetch, timeout, block=block)
                for message in messages:
                    callback(message)
            except redis.RedisError as e:
                logger.error(f"Error consuming messages: {e}")
                time.sleep(self.RECONNECTION_DELAY)

import logging
import threading
import uuid
from typing import Optional

import redis
import time

MAX_SEND_ATTEMPTS = 6  # 最大发送重试次数
MAX_CONNECTION_ATTEMPTS = float('inf')  # 最大连接重试次数
MAX_CONNECTION_DELAY = 2 ** 5  # 最大延迟时间

logger = logging.Logger(__name__)


class RedisStore:
    
    def __init__(self, *, host=None, port=None, password=None, **kwargs):
        """
        :param host: Redis host
        :param port: Redis port
        :param password: Redis password
        :param kwargs: Redis parameters
        """
        self.state = threading.local()
        self.parameters = {
            'host': host or 'localhost',
            'port': port or 6379,
            'password': password or None,
        }
        if kwargs:
            self.parameters.update(kwargs)
    
    def _create_connection(self):
        attempts = 1
        delay = 1
        while attempts <= MAX_CONNECTION_ATTEMPTS:
            try:
                connector = redis.Redis(**self.parameters)
                connector.ping()
                if attempts > 1:
                    logger.warning(f"RedisStore connection succeeded after {attempts} attempts", )
                return connector
            except redis.ConnectionError as exc:
                logger.warning(f"RedisStore connection error<{exc}>; retrying in {delay} seconds")
                attempts += 1
                time.sleep(delay)
                if delay < MAX_CONNECTION_DELAY:
                    delay *= 2
                    delay = min(delay, MAX_CONNECTION_DELAY)
        raise redis.ConnectionError("RedisStore connection error, max attempts reached")
    
    @property
    def connection(self):
        connection = getattr(self.state, "connection", None)
        if connection is None:
            connection = self.state.connection = self._create_connection()
        return connection
    
    @connection.deleter
    def connection(self):
        if _connection := getattr(self.state, "connection", None):
            try:
                _connection.close()
            except Exception as exc:
                logger.exception(f"RedisStore connection close error<{exc}>")
            del self.state.connection


class RedisStreamStore(RedisStore):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__shutdown = False
        self.__shutdown_event = threading.Event()
    
    def __del__(self):
        self.shutdown()
    
    def shutdown(self):
        self.__shutdown = True
        self.__shutdown_event.set()
    
    def _init_consume(self, stream_name, prefetch, timeout, **kwargs):
        self.state.stream = stream_name
        self.state.group = kwargs.get('group', 'store_default_group')
        self.state.group_start_id = kwargs.get('group_start_id',
                                               '0-0')  # 消费组起始ID，默认为0（从头开始消费），可设置为'$'（从最新消息开始消费），或者指定合法的消息ID
        self._create_group()
        
        self.state.consumer = kwargs.get('consumer', str(uuid.uuid4()))
        
        self.state.xclaim_interval = kwargs.get('xclaim_interval', 300000)  # xclaim 间隔时间，单位毫秒，默认 5 分钟
        self.state.xclaim_last_time = 0
        self.state.xclaim_start_id = '0-0'
        
        self.state.timeout = timeout or 3600000  # steam 的 消息 pending 超时时间，默认为 1 小时
        self.state.prefetch = prefetch or 1  # 默认预取 1 条消息
    
    def _create_group(self):
        try:
            self.connection.xgroup_create(self.state.stream, self.state.group, id=self.state.group_start_id,
                                          mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "already exists" not in str(e):
                raise e
    
    def _is_need_xclaim(self):
        """判断是否需要 xclaim"""
        return time.time() * 1000 - self.state.xclaim_last_time > self.state.xclaim_interval
    
    def _consume(self):
        """消费消息，返回消息列表"""
        # 获取 未ACK 消息数量
        pending = self.connection.xpending_range(
            self.state.stream, self.state.group,
            count=self.state.prefetch, idle=0,
            consumername=self.state.consumer
        )
        # 计算需获取的消息数量
        count = self.state.prefetch - len(pending) if pending else len(pending)
        if count <= 0:
            return []
        
        result = []
        # xclaim
        if self._is_need_xclaim():
            self.state.xclaim_start_id, messages, _ = self.connection.xautoclaim(
                self.state.stream, self.state.group, self.state.consumer,
                min_idle_time=self.state.timeout,
                start_id=self.state.xclaim_start_id,
                count=count
            )
            if messages:
                result.extend(messages)
            self.state.xclaim_last_time = time.time() * 1000
        # xreadgroup
        if len(result) < count:
            messages = self.connection.xreadgroup(
                self.state.group, self.state.consumer, {self.state.stream: '>'},
                count=count - len(result)
            )
            if messages:
                result.extend(messages)
        
        return result
    
    def start_consuming(self, stream_name, callback, prefetch=1, timeout=3600000, **kwargs):
        """开始消费
        
        :param stream_name: steam 名称
        :param callback: 消费回调函数
        :param prefetch: 预取消息数量
        :param timeout: steam 的消息 pending 超时时间，默认为 1 小时
        :param kwargs: 其他参数，详见源码 _init_consumer 方法
        """
        self.__shutdown = False
        self.__shutdown_event.clear()
        self._init_consume(stream_name=stream_name, prefetch=prefetch, timeout=timeout, **kwargs)
        
        while not self.__shutdown:
            if self.__shutdown:
                break
            try:
                messages = self._consume()
                for message in messages:
                    _, msg = message
                    callback(msg)
            except redis.ConnectionError:
                logger.warning("RedisStore consume connection error, reconnecting...")
                del self.connection
                time.sleep(1)
            except Exception as e:
                logger.exception(f"RedisStore consume error<{e}>, reconnecting...")
                del self.connection
                time.sleep(1)
    
    def send(self, stream, message, **kwargs):
        """发送消息"""
        attempts = 1
        while True:
            try:
                self.connection.xadd(stream, message, **kwargs)
                return message
            except Exception as exc:
                del self.connection
                attempts += 1
                if attempts > MAX_SEND_ATTEMPTS:
                    raise exc


useRedis = RedisStore
useRedisStreamStore = RedisStreamStore

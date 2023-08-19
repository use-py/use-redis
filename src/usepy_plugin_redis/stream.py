import logging
import uuid

import redis
import time

from .store import RedisStore

MAX_SEND_ATTEMPTS = 6  # 最大发送重试次数

logger = logging.Logger(__name__)


class RedisStreamStore(RedisStore):

    def __init__(self, stream_name, group_name=None, consumer_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__shutdown = False
        self.stream_name = stream_name
        self.group_name = group_name or 'default_group'
        self.consumer_name = consumer_name or str(uuid.uuid4())

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.__shutdown = True

    def _init_consume(self, prefetch, timeout, **kwargs):
        self.state.group_start_id = kwargs.get('group_start_id',
                                               '0-0')  # 消费组起始ID，默认为0（从头开始消费），可设置为'$'（从最新消息开始消费），或者指定合法的消息ID
        self._create_group()

        self.state.xclaim_interval = kwargs.get('xclaim_interval', 300000)  # xclaim 间隔时间，单位毫秒，默认 5 分钟
        self.state.xclaim_last_time = 0
        self.state.xclaim_start_id = '0-0'

        self.state.timeout = timeout or 3600000  # steam 的 消息 pending 超时时间，默认为 1 小时
        self.state.prefetch = prefetch or 1  # 默认预取 1 条消息

    def _create_group(self):
        try:
            self.connection.xgroup_create(self.stream_name, self.group_name, id=self.state.group_start_id,
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
            self.stream_name, self.group_name,
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
                self.stream_name, self.group_name, self.consumer_name,
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

    def start_consuming(self, callback, prefetch=1, timeout=3600000, **kwargs):
        """开始消费

        :param callback: 消费回调函数
        :param prefetch: 预取消息数量
        :param timeout: steam 的消息 pending 超时时间，默认为 1 小时
        :param kwargs: 其他参数，详见源码 _init_consumer 方法
        """
        self.__shutdown = False
        # self._init_consume(stream_name=stream_name, prefetch=prefetch, timeout=timeout, **kwargs)

        while not self.__shutdown:
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
            finally:
                if self.__shutdown:
                    break

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

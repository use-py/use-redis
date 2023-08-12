import logging
import threading

import redis
import time
from threading import local

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
        self.state = local()
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

    def _create_group(self, stream, group):
        try:
            self.connection.xgroup_create(stream, group, id='0', mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "already exists" not in str(e):
                raise e

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

    def start_consuming(self, stream, group, consumer, callback, prefetch=1, **kwargs):
        """开始消费"""
        self.__shutdown = False
        self.__shutdown_event.clear()
        self._create_group(stream, group)
        while not self.__shutdown:
            if self.__shutdown:
                break
            try:
                messages = self.connection.xreadgroup(group, consumer, {stream: '>'}, count=prefetch, **kwargs)
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


useRedis = RedisStore
useRedisStreamStore = RedisStreamStore

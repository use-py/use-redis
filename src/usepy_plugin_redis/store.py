import logging

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
        self.parameters = {
            'host': host or 'localhost',
            'port': port or 6379,
            'password': password or None,
        }
        if kwargs:
            self.parameters.update(kwargs)
        self._connection = None

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
        if self._connection is None:
            self._connection = self._create_connection()
        return self._connection

    @connection.deleter
    def connection(self):
        if self._connection:
            try:
                self._connection.close()
            except Exception as exc:
                logger.exception(f"RedisStore connection close error<{exc}>")
            self._connection = None

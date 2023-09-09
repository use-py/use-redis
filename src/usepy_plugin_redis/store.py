import logging
import time

import redis

logger = logging.Logger(__name__)


class RedisStore:
    MAX_SEND_ATTEMPTS = 6
    MAX_CONNECTION_ATTEMPTS = float('inf')
    MAX_CONNECTION_DELAY = 2 ** 5
    RECONNECTION_DELAY = 1

    def __init__(self, *, host=None, port=None, password=None, **kwargs):
        """
        :param host: Redis host
        :param port: Redis port
        :param password: Redis password
        :param kwargs: Redis parameters
        """
        self._shutdown = False
        self.parameters = {
            'host': host or 'localhost',
            'port': port or 6379,
            'password': password or None,
            'decode_responses': True,
        }
        if kwargs:
            self.parameters.update(kwargs)
        self._connection = None

    def _create_connection(self):
        attempts = 1
        reconnection_delay = self.RECONNECTION_DELAY
        while attempts <= self.MAX_CONNECTION_ATTEMPTS:
            try:
                connector = redis.Redis(**self.parameters)
                connector.ping()
                if attempts > 1:
                    logger.warning(f"RedisStore connection succeeded after {attempts} attempts", )
                return connector
            except redis.ConnectionError as exc:
                logger.warning(f"RedisStore connection error<{exc}>; retrying in {reconnection_delay} seconds")
                attempts += 1
                time.sleep(reconnection_delay)
                reconnection_delay = min(reconnection_delay * 2, self.MAX_CONNECTION_DELAY)
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

    def shutdown(self):
        self._shutdown = True
        del self.connection

    def __del__(self):
        self.shutdown()

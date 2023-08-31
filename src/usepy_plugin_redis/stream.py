import logging
import time

import redis

from .store import RedisStore

logger = logging.getLogger(__name__)


class RedisStreamStore(RedisStore):

    def __init__(self, stream_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_name = stream_name

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

    def _process_pending_messages(self, consumer_name, consumer_group, callback):
        """
        Process pending messages for the given consumer.
        """
        claim_start_id = "0-0"
        while True:
            try:
                pending_info = self.connection.xpending(self.stream_name, consumer_group)
                if pending_info.get('pending', 0) > 0:
                    start_id = pending_info['min']
                    end_id = pending_info['max']
                    # pending_messages = self.connection.xpending_range(self.stream_name, consumer_group, start_id,
                    #                                                   end_id, count=10)

                    claim_start_id, pending_messages, _ = self.connection.xautoclaim(
                        self.stream_name, consumer_group, consumer_name,
                        min_idle_time=0,
                        start_id=claim_start_id,
                        count=1
                    )
                    print(pending_messages)
                    for message in pending_messages:
                        callback([message], consumer_name, consumer_group)
                else:
                    break

            except redis.RedisError as e:
                logger.error(f"Error processing pending messages: {e}")
                time.sleep(self.RECONNECTION_DELAY)

    def start_consuming(self, consumer_name, consumer_group, callback, prefetch=1):
        """
        Start consuming messages from the Redis stream.
        """
        self._create_group(consumer_group)

        self._process_pending_messages(consumer_name, consumer_group, callback)

        while True:
            try:
                messages = self.connection.xreadgroup(consumer_group, consumer_name, {self.stream_name: '>'},
                                                      count=prefetch)
                for _, message in messages:
                    callback(message, consumer_name, consumer_group)
            except redis.RedisError as e:
                logger.error(f"Error consuming messages: {e}")
                time.sleep(self.RECONNECTION_DELAY)

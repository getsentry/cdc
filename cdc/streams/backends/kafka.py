import functools
import logging
from confluent_kafka import Producer

from cdc.common import Message
from cdc.logging import LoggerAdapter
from cdc.streams.backends import PublisherBackend


logger = LoggerAdapter(logging.getLogger(__name__))


class KafkaPublisherBackend(PublisherBackend):
    schema = {
        "type": "object",
        "properties": {
            "topic": {"type": "string"},
            "producer": {"type": "object", "properties": {}},  # TODO
        },
        "required": ["topic"],
    }

    def __init__(self, configuration):
        self.__topic = configuration["topic"]
        self.__producer = Producer(configuration["producer"])

    def __repr__(self):
        return "<{type}: {topic!r}>".format(
            type=type(self).__name__, topic=self.__topic
        )

    def __len__(self):
        return len(self.__producer)

    def validate(self):
        # TODO: Check the topic configuration, warn loudly if there are too
        # many partitions, etc.
        pass

    def __delivery_callback(self, id, position, callback, error, message):  # TODO: type
        if error is not None:
            raise Exception(error)
        callback(id, position)

    def write(self, message: Message, callback):  # TODO: type
        self.__producer.produce(
            self.__topic,
            message.payload,
            callback=functools.partial(
                self.__delivery_callback, message.id, message.position, callback
            ),
        )

    def poll(self, timeout: float):
        self.__producer.poll(timeout)

    def flush(self, timeout: float):
        self.__producer.flush(timeout)

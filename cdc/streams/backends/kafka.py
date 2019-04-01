import functools
import jsonschema
import logging
from confluent_kafka import KafkaError, Producer
from typing import Callable, Union

from cdc.logging import LoggerAdapter
from cdc.sources.types import Payload
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
        jsonschema.validate(configuration, self.schema)

        self.__topic: str = configuration["topic"]
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

    def __delivery_callback(
        self,
        callback: Callable[[], None],
        error: Union[None, KafkaError],
        *args,
        **kwargs
    ):
        if error is not None:
            raise Exception(error)
        callback()

    def write(self, payload: Payload, callback: Callable[[], None]):
        self.__producer.produce(
            self.__topic,
            payload,
            callback=functools.partial(self.__delivery_callback, callback),
        )

    def poll(self, timeout: float):
        self.__producer.poll(timeout)

    def flush(self, timeout: float):
        self.__producer.flush(timeout)

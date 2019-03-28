import functools
import logging
from abc import ABC, abstractmethod
from typing import Union

from cdc.common import Message
from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


backends = {}


class Publisher(object):
    schema = {
        "type": "object",
        "properties": {
            "backend": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "options": {"type": "object"},
                },
                "required": ["type"],
            }
        },
        "required": ["backend"],
    }

    def __init__(self, configuration):
        self.__backend = backends[configuration["backend"]["type"]](
            configuration["backend"]["options"]
        )

    def __repr__(self):
        return "<{type}: {backend}>".format(
            type=type(self).__name__, backend=self.__backend
        )

    def __len__(self) -> int:
        return len(self.__backend)

    def write(self, message: Message, callback):  # TODO: type
        self.__backend.write(message, callback)

    def poll(self, timeout: float):
        self.__backend.poll(timeout)

    def flush(self, timeout: float):
        self.__backend.flush(timeout)


class PublisherBackend(ABC):
    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def write(self, message: Message, callback):  # TODO: type
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float):
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: float):
        raise NotImplementedError


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
        from confluent_kafka import Producer

        self.__topic = configuration["topic"]
        self.__producer = Producer(configuration["producer"])

    def __repr__(self):
        return "<{type}: {topic!r}>".format(
            type=type(self).__name__, topic=self.__topic
        )

    def __len__(self):
        return len(self.__producer)

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


backends["kafka"] = KafkaPublisherBackend

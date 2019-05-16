import functools
import jsonschema  # type: ignore
import logging
from confluent_kafka import KafkaError, Producer  # type: ignore
from typing import Any, Callable, Mapping, Union

from cdc.sources.types import Payload
from cdc.streams.backends import ProducerBackend
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration


logger = LoggerAdapter(logging.getLogger(__name__))


class KafkaProducerBackend(ProducerBackend):
    def __init__(self, topic: str, options: Mapping[str, Any]):
        self.__topic = topic
        self.__producer = Producer(options)

    def __repr__(self) -> str:
        return "<{type}: {topic!r}>".format(
            type=type(self).__name__, topic=self.__topic
        )

    def __len__(self) -> int:
        return len(self.__producer)

    def __delivery_callback(
        self,
        callback: Callable[[], None],
        error: Union[None, KafkaError],
        *args,
        **kwargs
    ) -> None:
        if error is not None:
            raise Exception(error)
        callback()

    def write(self, payload: Payload, callback: Callable[[], None]) -> None:
        self.__producer.produce(
            self.__topic,
            payload,
            callback=functools.partial(self.__delivery_callback, callback),
        )

    def poll(self, timeout: float) -> None:
        self.__producer.poll(timeout)

    def flush(self, timeout: float) -> None:
        self.__producer.flush(timeout)


def kafka_producer_backend_factory(
    configuration: Configuration
) -> KafkaProducerBackend:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "options": {"type": "object", "properties": {}},  # TODO
            },
            "required": ["topic"],
        },
    )
    return KafkaProducerBackend(
        topic=configuration["topic"], options=configuration["options"]
    )

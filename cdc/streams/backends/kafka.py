import functools
import logging
from typing import Any, Callable, Mapping, Optional

import jsonschema  # type: ignore
from cdc.sources.types import ChangeMessage, ReplicationEvent
from cdc.streams.backends import ProducerBackend
from cdc.streams.types import StreamMessage
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration
from confluent_kafka import KafkaError, Producer  # type: ignore

logger = LoggerAdapter(logging.getLogger(__name__))


class KafkaProducerBackend(ProducerBackend):
    """
    Provides a producer backend implementation that writes to a Kafka topic.
    """

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
        error: Optional[KafkaError],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if error is not None:
            raise Exception(error)
        callback()

    def write(self, msg: StreamMessage, callback: Callable[[], None]) -> None:
        self.__producer.produce(
            self.__topic,
            msg.payload,
            callback=functools.partial(self.__delivery_callback, callback),
            headers=msg.metadata if msg.metadata is not None else {},
        )

    def poll(self, timeout: float) -> None:
        self.__producer.poll(timeout)

    def flush(self, timeout: float) -> int:
        return self.__producer.flush(timeout)


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

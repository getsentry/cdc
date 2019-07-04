import functools
import logging
from confluent_kafka import KafkaError, Producer  # type: ignore
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional

from cdc.sources.types import Payload
from cdc.streams.backends import ProducerBackend
from cdc.utils.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


@dataclass(frozen=True)
class Configuration:
    topic: str
    options: Mapping[str, Any] = field(default_factory=dict)


class KafkaProducerBackend(ProducerBackend):
    """
    Provides a producer backend implementation that writes to a Kafka topic.
    """

    def __init__(self, configuration: Configuration):
        self.__configuration = configuration
        self.__producer = Producer(configuration.options)

    def __repr__(self) -> str:
        return "<{type}: {topic!r}>".format(
            type=type(self).__name__, topic=self.__configuration.topic
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

    def write(self, payload: Payload, callback: Callable[[], None]) -> None:
        self.__producer.produce(
            self.__configuration.topic,
            payload,
            callback=functools.partial(self.__delivery_callback, callback),
        )

    def poll(self, timeout: float) -> None:
        self.__producer.poll(timeout)

    def flush(self, timeout: float) -> None:
        self.__producer.flush(timeout)

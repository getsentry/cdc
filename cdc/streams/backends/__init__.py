import logging
from abc import ABC, abstractmethod
from typing import Callable

from cdc.logging import LoggerAdapter
from cdc.sources.types import Payload


logger = LoggerAdapter(logging.getLogger(__name__))


class PublisherBackend(ABC):
    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    def validate(self) -> None:
        logger.trace("Validation is not implemented for %r.", self)

    @abstractmethod
    def write(self, payload: Payload, callback: Callable[[], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: float) -> None:
        raise NotImplementedError


from cdc.registry import Registry
from cdc.streams.backends.kafka import kafka_publisher_backend_factory

publisher_registry: Registry[PublisherBackend] = Registry(
    {"kafka": kafka_publisher_backend_factory}
)

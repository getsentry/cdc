import logging
from abc import ABC, abstractmethod
from typing import Callable

from cdc.logging import LoggerAdapter
from cdc.sources.types import Payload


logger = LoggerAdapter(logging.getLogger(__name__))


class ProducerBackend(ABC):
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
from cdc.streams.backends.kafka import kafka_producer_backend_factory

producer_registry: Registry[ProducerBackend] = Registry(
    {"kafka": kafka_producer_backend_factory}
)

import logging
from abc import ABC, abstractmethod
from typing import Callable

from cdc.sources.types import Payload
from cdc.utils.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


class ProducerBackend(ABC):
    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def write(self, payload: Payload, callback: Callable[[], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: float) -> None:
        raise NotImplementedError


from cdc.utils.registry import Registry
from cdc.streams.backends.kafka import kafka_producer_backend_factory

producer_registry: Registry[ProducerBackend] = Registry(
    {"kafka": kafka_producer_backend_factory}
)

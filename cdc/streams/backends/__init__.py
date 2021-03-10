import logging
from abc import ABC, abstractmethod
from typing import Callable, Mapping, NamedTuple

from cdc.streams.types import StreamMessage
from cdc.utils.logging import LoggerAdapter

logger = LoggerAdapter(logging.getLogger(__name__))


class ProducerBackend(ABC):
    """
    Abstract base class for producer backend implementations.

    For more details on the expected behavior of individual methods, see the
    documentation on ``Producer``.
    """

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def write(self, payload: StreamMessage, callback: Callable[[], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: float) -> int:
        raise NotImplementedError


from cdc.streams.backends.kafka import kafka_producer_backend_factory
from cdc.utils.registry import Registry

producer_registry: Registry[ProducerBackend] = Registry(
    {"kafka": kafka_producer_backend_factory}
)

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Tuple

from cdc.sources.types import ReplicationEvent, Position
from cdc.types import ScheduledTask
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Registry


logger = LoggerAdapter(logging.getLogger(__name__))


class SourceBackend(ABC):
    """
    Abstract base class for source backend implementations.

    For more details on the expected behavior of individual methods, see the
    documentation on ``Source``.
    """

    @abstractmethod
    def fetch(self) -> Optional[ReplicationEvent]:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def commit_positions(
        self, write_position: Optional[Position], flush_position: Optional[Position]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_next_scheduled_task(self, now: datetime) -> Optional[ScheduledTask]:
        raise NotImplementedError


from cdc.sources.backends.postgres_logical import postgres_logical_factory

registry: Registry[SourceBackend] = Registry(
    {"postgres_logical": postgres_logical_factory}
)

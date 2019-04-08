import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Tuple, Union

from cdc.sources.types import Payload, Position
from cdc.types import ScheduledTask
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Registry


logger = LoggerAdapter(logging.getLogger(__name__))


class SourceBackend(ABC):
    def validate(self) -> None:
        logger.trace("Validation is not implemented for %r.", self)

    @abstractmethod
    def fetch(self) -> Union[None, Tuple[Position, Payload]]:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def commit_positions(
        self,
        write_position: Union[None, Position],
        flush_position: Union[None, Position],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_next_scheduled_task(self, now: datetime) -> Union[None, ScheduledTask]:
        raise NotImplementedError


from cdc.sources.backends.postgres_logical import postgres_logical_factory

registry: Registry[SourceBackend] = Registry(
    {"postgres_logical": postgres_logical_factory}
)

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Mapping, Union, Tuple, Type

from cdc.common import Position, ScheduledTask
from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


class SourceBackend(ABC):
    def validate(self):
        logger.trace("Validation is not implemented for %r.", self)

    @abstractmethod
    def fetch(self) -> Union[None, Tuple[Position, str]]:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float):
        raise NotImplementedError

    @abstractmethod
    def commit_positions(
        self,
        write_position: Union[None, Position],
        flush_position: Union[None, Position],
    ):
        raise NotImplementedError

    @abstractmethod
    def get_next_scheduled_task(self, now: datetime) -> Union[None, ScheduledTask]:
        raise NotImplementedError


from cdc.sources.backends.postgres_logical import PostgresLogicalReplicationSlotBackend

registry: Mapping[str, Type[SourceBackend]] = {
    'postgres_logical': PostgresLogicalReplicationSlotBackend,
}

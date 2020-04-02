import itertools
import jsonschema  # type: ignore
import logging
from datetime import datetime, timedelta
from typing import Optional

from cdc.sources.backends import SourceBackend, registry
from cdc.sources.types import Id, CdcMessage, Position
from cdc.types import ScheduledTask
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration


logger = LoggerAdapter(logging.getLogger(__name__))


class Source(object):
    """
    Source for replication messages. This class also is reponsible for managing
    replication log positions.

    This class contains the interfaces to common functionality for all types of
    generic replication message sources. The specific details are delegated to
    the backend implementation.
    """

    COMMIT_TASK = "commit_position"

    def __init__(
        self,
        backend: SourceBackend,
        commit_positions_after_seconds: Optional[float] = None,
        commit_positions_after_flushed_messages: Optional[int] = None,
    ):
        if commit_positions_after_seconds is None:
            commit_positions_after_seconds = 60.0

        self.__backend = backend

        # The maximum number of flushed messages between committing positions.
        self.__commit_messages = commit_positions_after_flushed_messages

        # The maximum number of seconds to wait between committing positions.
        self.__commit_timeout = commit_positions_after_seconds

        self.__id_generator = itertools.count(1)

        self.__write_id: Optional[Id] = None
        self.__write_position: Optional[Position] = None

        self.__flush_id: Optional[Id] = None
        self.__flush_position: Optional[Position] = None

        self.__last_commit_flush_id: Optional[Id] = None
        self.__last_commit_datetime: datetime = datetime.now()  # TODO: This is kind of a strange default

    def __repr__(self) -> str:
        return "<{type}: {backend}>".format(
            type=type(self).__name__, backend=self.__backend
        )

    def fetch(self) -> Optional[CdcMessage]:
        """
        Attempts to fetch the next message from the source backend. If no
        message is ready, ``None`` is returned instead.

        This method should not block.
        """
        result = self.__backend.fetch()
        if result is not None:
            return CdcMessage(Id(next(self.__id_generator)), result)
        else:
            return None

    def poll(self, timeout: float) -> None:
        """
        Waits until the a message is ready to be fetched from the source
        backend or the timeout is reached.
        """
        self.__backend.poll(timeout)

    def set_write_position(self, id: Id, position: Position) -> None:
        """
        Sets the current write position.

        The position passed to this method represents the last message that was
        written to the destination but has not guaranteed to have been written
        durably.
        """
        logger.trace("Updating write position of %r to %s...", self, position)
        assert (self.__write_id or 0) + 1 == id
        self.__write_id = id
        self.__write_position = position

    def set_flush_position(self, id: Id, position: Position) -> None:
        """
        Sets the current flush position.

        The position passed to this method represents the last message that was
        written to the destination and has been guaranteed to be have been
        written durably.
        """
        logger.trace("Updating flush position of %r to %s...", self, position)
        assert (self.__flush_id or 0) + 1 == id
        self.__flush_id = id
        self.__flush_position = position

    def commit_positions(self) -> None:
        """
        Commits the current write and flush positions to the source.
        """
        logger.trace("Committing positions...")
        self.__backend.commit_positions(self.__write_position, self.__flush_position)
        logger.debug(
            "Updated committed positions: write=%r, flush=%r",
            self.__write_position,
            self.__flush_position,
        )
        self.__last_commit_flush_id = self.__flush_id
        self.__last_commit_datetime = datetime.now()

    def get_next_scheduled_task(self, now: datetime) -> ScheduledTask:
        """
        Returns the next scheduled task to be performed.
        """
        if (
            self.__commit_messages is not None
            and self.__flush_id is not None
            and self.__flush_id
            - (
                self.__last_commit_flush_id
                if self.__last_commit_flush_id is not None
                else 0
            )
            > self.__commit_messages
        ):
            return ScheduledTask(now, self.commit_positions, self.COMMIT_TASK)

        task = ScheduledTask(
            self.__last_commit_datetime + timedelta(seconds=self.__commit_timeout),
            self.commit_positions,
            self.COMMIT_TASK,
        )

        backend_task = self.__backend.get_next_scheduled_task(now)
        if backend_task is not None and task > backend_task:
            task = backend_task

        return task


def source_factory(configuration: Configuration) -> Source:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {
                "backend": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "options": {"type": "object"},
                    },
                    "required": ["type"],
                },
                "commit_positions_after_flushed_messages": {"type": "number"},
                "commit_positions_after_seconds": {"type": "number"},
            },
            "required": ["backend"],
        },
    )
    return Source(
        backend=registry.new(
            configuration["backend"]["type"], configuration["backend"]["options"]
        ),
        commit_positions_after_flushed_messages=configuration.get(
            "commit_positions_after_flushed_messages"
        ),
        commit_positions_after_seconds=configuration.get(
            "commit_positions_after_seconds"
        ),
    )

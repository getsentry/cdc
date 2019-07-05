import logging
import psycopg2  # type: ignore
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from psycopg2.extensions import cursor  # type: ignore
from psycopg2.extras import (  # type: ignore
    LogicalReplicationConnection,
    REPLICATION_LOGICAL,
)
from select import select
from typing import Mapping, Optional, Tuple

from cdc.sources.backends import SourceBackend
from cdc.sources.types import Payload, Position
from cdc.types import ScheduledTask
from cdc.utils.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


@dataclass(frozen=True)
class Slot:
    # The name of the replication slot.
    name: str

    # The output plugin used by the replication slot. Only used when creating a replication slot.
    plugin: str

    # Whether or not to attempt to create the replication on startup.
    create: bool = False

    # The options used by the replication slot's output plugin.
    options: Mapping[str, str] = field(default_factory=dict)


class PostgresLogicalReplicationSlotBackend(SourceBackend):
    """
    Provides a source backend implementation backed by PostgreSQL's logical
    replication slot concepts.
    """

    def __init__(self, dsn: str, slot: Slot, keepalive_interval: float = 10.0):
        self.__dsn = dsn

        self.__slot = slot

        # How many seconds to wait between scheduling keepalive messages to be sent.
        self.__keepalive_interval: float = 10.0

        self.__last_keepalive_datetime: datetime = datetime.now()
        self.__cursor: cursor = None

    def __repr__(self) -> str:
        return "<{type}: {slot!r} on {dsn!r}>".format(
            type=type(self).__name__, slot=self.__slot.name, dsn=self.__dsn
        )

    def __get_cursor(self, create: bool = False) -> cursor:
        if self.__cursor is not None:
            return self.__cursor
        elif not create:
            raise Exception("cursor not already established")

        logger.debug("Establishing replication connection to %r...", self.__dsn)
        self.__cursor = psycopg2.connect(
            self.__dsn, connection_factory=LogicalReplicationConnection
        ).cursor()

        if self.__slot.create:
            logger.debug(
                "Creating replication slot %r using %r, if it doesn't already exist...",
                self.__slot.name,
                self.__slot.plugin,
            )
            try:
                self.__cursor.create_replication_slot(
                    self.__slot.name, REPLICATION_LOGICAL, self.__slot.plugin
                )
            except psycopg2.ProgrammingError as e:
                if (
                    str(e).strip()
                    == f'replication slot "{self.__slot.name}" already exists'
                ):
                    logger.debug("Replication slot already exists.")
                else:
                    raise
            else:
                logger.debug("Replication slot created.")

        logger.debug("Starting replication on %r...", self.__cursor)
        self.__cursor.start_replication(
            self.__slot.name, REPLICATION_LOGICAL, options=self.__slot.options
        )

        return self.__cursor

    def fetch(self) -> Optional[Tuple[Position, Payload]]:
        message = self.__get_cursor(create=True).read_message()
        if message is not None:
            return (Position(message.data_start), Payload(message.payload))
        else:
            return None

    def poll(self, timeout: float) -> None:
        select([self.__get_cursor()], [], [], timeout)

    def commit_positions(
        self, write_position: Optional[Position], flush_position: Optional[Position]
    ) -> None:
        send_feedback_kwargs = {}

        if write_position is not None:
            send_feedback_kwargs["write_lsn"] = write_position

        if flush_position is not None:
            send_feedback_kwargs["flush_lsn"] = flush_position

        self.__get_cursor().send_feedback(**send_feedback_kwargs)
        self.__last_keepalive_datetime = datetime.now()

    def send_keepalive(self) -> None:
        """
        Send a keep-alive message.
        """
        self.__get_cursor().send_feedback()
        self.__last_keepalive_datetime = datetime.now()

    def get_next_scheduled_task(self, now: datetime) -> Optional[ScheduledTask]:
        return ScheduledTask(
            self.__last_keepalive_datetime
            + timedelta(seconds=self.__keepalive_interval),
            self.send_keepalive,
            "keepalive",
        )

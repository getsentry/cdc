import logging
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import LogicalReplicationConnection, REPLICATION_LOGICAL
from select import select
from typing import Union, Tuple

from cdc import ScheduledTask
from cdc.common import Position
from cdc.logging import LoggerAdapter
from cdc.sources.backends import SourceBackend


logger = LoggerAdapter(logging.getLogger(__name__))


class PostgresLogicalReplicationSlotBackend(SourceBackend):
    schema = {
        "type": "object",
        "properties": {
            "dsn": {"type": "string"},
            "slot": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "plugin": {"type": "string"},
                    "create": {"type": "boolean"},
                    "options": {"type": "object", "properties": {}},  # TODO
                },
                "required": ["name", "plugin"],
            },
            "keepalive_interval": {"type": "number"},
        },
        "required": ["dsn"],
    }

    def __init__(self, configuration):
        self.__dsn = configuration["dsn"]
        self.__slot_name = configuration["slot"]["name"]
        self.__slot_plugin = configuration["slot"]["plugin"]
        self.__slot_options = configuration["slot"]["options"]
        self.__slot_create = configuration["slot"]["create"]
        self.__keepalive_interval = configuration["keepalive_interval"]
        self.__cursor = None  # TODO: type

    def __repr__(self):
        return "<{type}: {slot!r} on {dsn!r}>".format(
            type=type(self).__name__, slot=self.__slot_name, dsn=self.__dsn
        )

    def __get_cursor(self, create: bool = False):
        if self.__cursor is not None:
            return self.__cursor
        elif not create:
            raise Exception("cursor not already established")

        logger.debug("Establishing replication connection to %r...", self.__dsn)
        self.__cursor = psycopg2.connect(
            self.__dsn, connection_factory=LogicalReplicationConnection
        ).cursor()

        if self.__slot_create:
            logger.debug(
                "Creating replication slot %r using %r, if it doesn't already exist...",
                self.__slot_name,
                self.__slot_plugin,
            )
            try:
                self.__cursor.create_replication_slot(
                    self.__slot_name, REPLICATION_LOGICAL, self.__slot_plugin
                )
            except psycopg2.ProgrammingError:
                logger.debug(
                    "Failed to create replication slot -- assuming it already exists.",
                    exc_info=True,
                )
            else:
                logger.debug("Replication slot created.")

        logger.debug("Starting replication on %r...", self.__cursor)
        self.__cursor.start_replication(
            self.__slot_name, REPLICATION_LOGICAL, options=self.__slot_options
        )

        return self.__cursor

    def fetch(self) -> Union[None, Tuple[Position, str]]:
        message = self.__get_cursor(create=True).read_message()
        if message is not None:
            return (Position(message.data_start), message.payload)
        else:
            return None

    def poll(self, timeout: float):
        select([self.__get_cursor()], [], [], timeout)

    def commit_positions(
        self,
        write_position: Union[None, Position],
        flush_position: Union[None, Position],
    ):
        send_feedback_kwargs = {}

        if write_position is not None:
            send_feedback_kwargs["write_lsn"] = write_position

        if flush_position is not None:
            send_feedback_kwargs["flush_lsn"] = flush_position

        self.__get_cursor().send_feedback(**send_feedback_kwargs)

    def send_keepalive(self):
        """
        Send a keep-alive message.
        """
        self.__get_cursor().send_feedback()

    def get_next_scheduled_task(self, now: datetime) -> Union[None, ScheduledTask]:
        return ScheduledTask(
            self.__get_cursor(create=False).io_timestamp
            + timedelta(seconds=self.__keepalive_interval),
            self.send_keepalive,
        )

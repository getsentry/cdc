import jsonschema
import logging
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extensions import cursor
from psycopg2.extras import LogicalReplicationConnection, REPLICATION_LOGICAL
from select import select
from typing import Mapping, Union, Tuple

from cdc.logging import LoggerAdapter
from cdc.registry import Configuration
from cdc.sources.backends import SourceBackend
from cdc.sources.types import Payload, Position
from cdc.types import ScheduledTask


logger = LoggerAdapter(logging.getLogger(__name__))


class PostgresLogicalReplicationSlotBackend(SourceBackend):
    def __init__(
        self,
        dsn: str,
        slot_name: str,
        slot_plugin: str,
        slot_options: Union[Mapping[str, str], None],
        slot_create: Union[None, bool] = None,
        keepalive_interval: Union[None, float] = None,
    ):
        if slot_options is None:
            slot_options = {}

        if slot_create is None:
            slot_create = False

        if keepalive_interval is None:
            keepalive_interval = 10.0

        self.__dsn = dsn
        self.__slot_name = slot_name
        self.__slot_plugin = slot_plugin
        self.__slot_options = slot_options
        self.__slot_create = slot_create
        self.__keepalive_interval = keepalive_interval

        self.__cursor: cursor = None

    def __repr__(self) -> str:
        return "<{type}: {slot!r} on {dsn!r}>".format(
            type=type(self).__name__, slot=self.__slot_name, dsn=self.__dsn
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

    def fetch(self) -> Union[None, Tuple[Position, Payload]]:
        message = self.__get_cursor(create=True).read_message()
        if message is not None:
            return (Position(message.data_start), Payload(message.payload))
        else:
            return None

    def poll(self, timeout: float) -> None:
        select([self.__get_cursor()], [], [], timeout)

    def commit_positions(
        self,
        write_position: Union[None, Position],
        flush_position: Union[None, Position],
    ) -> None:
        send_feedback_kwargs = {}

        if write_position is not None:
            send_feedback_kwargs["write_lsn"] = write_position

        if flush_position is not None:
            send_feedback_kwargs["flush_lsn"] = flush_position

        self.__get_cursor().send_feedback(**send_feedback_kwargs)

    def send_keepalive(self) -> None:
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


def postgres_logical_factory(
    configuration: Configuration
) -> PostgresLogicalReplicationSlotBackend:
    jsonschema.validate(
        configuration,
        {
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
        },
    )
    return PostgresLogicalReplicationSlotBackend(
        dsn=configuration["dsn"],
        slot_name=configuration["slot"]["name"],
        slot_plugin=configuration["slot"]["plugin"],
        slot_create=configuration["slot"].get("create"),
        slot_options=configuration["slot"].get("options"),
        keepalive_interval=configuration.get("keepalive_interval"),
    )

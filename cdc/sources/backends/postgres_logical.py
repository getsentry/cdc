import jsonschema  # type: ignore
import logging
import psycopg2  # type: ignore
from datetime import datetime, timedelta
from psycopg2.extensions import cursor  # type: ignore
from psycopg2.extras import (  # type: ignore
    LogicalReplicationConnection,
    REPLICATION_LOGICAL,
)
from select import select
from typing import Callable, Mapping, Optional

from cdc.sources.backends import SourceBackend
from cdc.sources.types import (
    BeginMessage,
    ChangeMessage,
    CommitMessage,
    GenericMessage,
    Payload,
    Position,
    ReplicationEvent,
)
from cdc.types import ScheduledTask
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration


logger = LoggerAdapter(logging.getLogger(__name__))

WalMessageParser = Callable[[int, Payload], ReplicationEvent]


def parse_generic_message(data_start: int, payload: Payload) -> ReplicationEvent:
    return GenericMessage(Position(data_start), Payload(payload))


def parse_message_with_headers(data_start: int, payload: Payload) -> ReplicationEvent:
    """
    Parses a message produced by wal2json in this format:
    TYPE|HEADERS|JSON
    or falls back to a GenericMessage if the input is not
    following the format above.
    """

    # Intentionally not parsing the full expression through a regex because
    # we do not need to parse the whole message and because there is no
    # constraint on what the payload section can contain making the regex
    # uselessly complex.
    if payload[:2] == b"B|":
        return BeginMessage(Position(data_start), Payload(payload[2:]))
    elif payload[:2] == b"C|":
        return CommitMessage(Position(data_start), Payload(payload[2:]))
    elif payload[:2] == b"G|":
        return GenericMessage(Position(data_start), Payload(payload[2:]))
    elif payload[:2] == b"M|":
        payload_without_type = payload[2:]
        consuming_json = False
        json_start_position = 0
        escape = False
        while not consuming_json:
            if chr(payload_without_type[json_start_position]) == "\\":
                escape = not escape
            else:
                if chr(payload_without_type[json_start_position]) == "|" and not escape:
                    consuming_json = True
                escape = False
            json_start_position = json_start_position + 1

        table_name = payload_without_type[: json_start_position - 1].decode(
            "utf-8", "strict"
        )
        table_name = table_name.replace("\\\\", "\\").replace("\\|", "|")
        return ChangeMessage(
            Position(data_start),
            Payload(payload_without_type[json_start_position:]),
            table_name,
        )
    else:
        return GenericMessage(Position(data_start), Payload(payload))


class PostgresLogicalReplicationSlotBackend(SourceBackend):
    """
    Provides a source backend implementation backed by PostgreSQL's logical
    replication slot concepts.
    """

    def __init__(
        self,
        dsn: str,
        wal_msg_parser: WalMessageParser,
        slot_name: str,
        slot_plugin: str,
        slot_options: Optional[Mapping[str, str]] = None,
        slot_create: Optional[bool] = None,
        keepalive_interval: Optional[float] = None,
    ):
        if slot_options is None:
            slot_options = {}

        if slot_create is None:
            slot_create = False

        if keepalive_interval is None:
            keepalive_interval = 10.0

        self.__dsn = dsn

        self.__wal_msg_parser = wal_msg_parser

        # The name of the replication slot.
        self.__slot_name = slot_name

        # The output plugin used by the replication slot. Only used when
        # creating a replication slot.
        self.__slot_plugin = slot_plugin

        # The options used by the replication slot's output plugin.
        self.__slot_options = slot_options

        # Whether or not to attempt to create the replication on startup.
        self.__slot_create = slot_create

        # How many seconds to wait between scheduling keepalive messages to be
        # sent.
        self.__keepalive_interval = keepalive_interval
        self.__last_keepalive_datetime: datetime = datetime.now()

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
            except psycopg2.ProgrammingError as e:
                if (
                    str(e).strip()
                    == f'replication slot "{self.__slot_name}" already exists'
                ):
                    logger.debug("Replication slot already exists.")
                else:
                    raise
            else:
                logger.debug("Replication slot created.")

        logger.debug("Starting replication on %r...", self.__cursor)
        self.__cursor.start_replication(
            self.__slot_name, REPLICATION_LOGICAL, options=self.__slot_options
        )

        return self.__cursor

    def fetch(self) -> Optional[ReplicationEvent]:
        message = self.__get_cursor(create=True).read_message()
        if message is not None:
            return self.__wal_msg_parser(message.data_start, message.payload)
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


def wal_msg_parser_factory(slot_config: Configuration) -> WalMessageParser:
    parser_type = slot_config.get("parser", "default")
    if parser_type == "default":
        options = slot_config.get("options", {})
        include_headers = options.get("include-message-header", "false")
        assert (
            include_headers == "false"
        ), "Invalid slot config. Cannot run default parser with include-message-header option."
        return parse_generic_message
    elif parser_type == "wal2json_parser_with_headers":
        return parse_message_with_headers
    raise ValueError(f"Parser type not defined: {parser_type}")


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
                        "parser": {"type": "string", "default": "default"},
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
        wal_msg_parser=wal_msg_parser_factory(configuration["slot"]),
        slot_name=configuration["slot"]["name"],
        slot_plugin=configuration["slot"]["plugin"],
        slot_create=configuration["slot"].get("create"),
        slot_options=configuration["slot"].get("options"),
        keepalive_interval=configuration.get("keepalive_interval"),
    )

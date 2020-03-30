import json  # type: ignore
import jsonschema  # type: ignore
import logging

from functools import partial
from typing import Optional, Sequence
from uuid import UUID

from cdc.snapshots.control_protocol import ControlMessage, SnapshotAbort, SnapshotInit
from cdc.snapshots.snapshot_types import SnapshotId
from cdc.sources.types import Payload
from cdc.streams import Producer as StreamProducer
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


class ProducerQueueNotEmpty(Exception):
    pass


class SnapshotControl:
    """
    Sends messages on the CDC control topic.
    """

    DEFAULT_FLUSH_TIMEOUT = 10

    def __init__(
        self, producer: StreamProducer, control_settings: Optional[Configuration]
    ) -> None:
        self.__producer = producer
        if not control_settings:
            control_settings = {}

        jsonschema.validate(
            control_settings,
            {
                "type": "object",
                "properties": {"flush_timeout": {"type": "number"}},
                "required": [],
            },
        )
        flush_timeout = control_settings.get("flush_timeout")
        self.__flush_timeout = (
            flush_timeout if flush_timeout is not None else self.DEFAULT_FLUSH_TIMEOUT
        )

    def wait_messages_sent(self) -> None:
        messages_in_queue = self.__producer.flush(self.__flush_timeout)
        if messages_in_queue > 0:
            raise ProducerQueueNotEmpty(
                f"The producer queue is not empty after flush timed out. "
                f"Messages still in queue: {messages_in_queue}"
            )

    def __msg_sent(self, msg: ControlMessage) -> None:
        logger.debug("Message sent %r", msg)

    def __write_msg(self, message: ControlMessage) -> None:
        json_string = json.dumps(message.to_dict())
        self.__producer.write(
            payload=Payload(json_string.encode("utf-8")),
            callback=partial(self.__msg_sent, message),
        )

    def init_snapshot(
        self, snapshot_id: UUID, tables: Sequence[str], product: str
    ) -> None:
        init_message = SnapshotInit(
            tables=tables, snapshot_id=SnapshotId(str(snapshot_id)), product=product
        )
        self.__write_msg(init_message)

    def abort_snapshot(self, snapshot_id: UUID) -> None:
        abort_message = SnapshotAbort(snapshot_id=SnapshotId(str(snapshot_id)))
        self.__write_msg(abort_message)

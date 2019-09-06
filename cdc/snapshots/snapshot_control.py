import json # type: ignore
import logging

from functools import partial
from typing import Sequence
from uuid import UUID

from cdc.snapshots.control_protocol import (
    ControlMessage,
    SnapshotAbort,
    SnapshotInit,
)
from cdc.snapshots.snapshot_types import SnapshotId, TableConfig
from cdc.sources.types import Message, Payload
from cdc.streams import Producer as StreamProducer
from cdc.utils.logging import LoggerAdapter

logger = LoggerAdapter(logging.getLogger(__name__))

class ProducerQueueNotEmpty(Exception):
    pass

class SnapshotControl:
    """
    Sends messages on the CDC control topic.
    """

    def __init__(
        self,
        producer: StreamProducer,
        poll_timeout: int,
    ) -> None:
        self.__producer = producer
        self.__poll_timeout = poll_timeout

    def wait_messages_sent(self) -> None:
        messages_in_queue = self.__producer.flush(self.__poll_timeout)
        if messages_in_queue > 0:
            raise ProducerQueueNotEmpty(
                f"The producer queue is not empty after flush timed out. "
                "{messages_in_queue} messages still in queue." 
            )


    def __msg_sent(self, msg: ControlMessage) -> None:
        logger.debug(
            "Message sent %r",
            msg,
        )

    def __write_msg(self, message: ControlMessage) -> None:
        json_string = json.dumps(message.to_dict())
        self.__producer.write(
            payload=Payload(json_string.encode("utf-8")),
            callback=partial(self.__msg_sent, message)
        )

    def init_snapshot(self,
        snapshot_id: UUID,
        tables: Sequence[str],
        product: str,
    ) -> None:
        init_message = SnapshotInit(
            tables=tables,
            snapshot_id=SnapshotId(str(snapshot_id)),
            product=product,
        )
        self.__write_msg(init_message)

    def abort_snapshot(self, snapshot_id: UUID) -> None:
        abort_message = SnapshotAbort(
            snapshot_id=SnapshotId(str(snapshot_id)),
        )
        self.__write_msg(abort_message)

import json # type: ignore
import logging

from functools import partial
from uuid import UUID

from cdc.snapshots.control_protocol import (
    ControlMessage,
    SnapshotAbort,
    SnapshotInit,
)
from cdc.snapshots.snapshot_types import SnapshotId, TableConfig
from cdc.sources.types import Message
from cdc.streams import Producer as StreamProducer
from cdc.utils.logging import LoggerAdapter

logger = LoggerAdapter(logging.getLogger(__name__))

class SnapshotControl:

    def __init__(
        self,
        producer: StreamProducer,
        poll_timeout: int,
    ) -> None:
        self.__producer = producer
        self.__poll_timeout = poll_timeout

    def wait_messages_sent(self) -> None:
        self.__producer.poll(self.__poll_timeout)

    def __msg_sent(self, msg: ControlMessage) -> None:
        logger.debug(
            "Message sent %r",
            msg,
        )


    def __write_msg(self, message: ControlMessage) -> None:
        self.__producer.write(
            payload=json.dumps(message.serialize()),
            callback=partial(self.__msg_sent, message)
        )

    def init_snapshot(self, snapshot_id: UUID, product: str) -> None:
        init_message = SnapshotInit(
            snapshot_id=SnapshotId(str(snapshot_id)),
            product=product,
        )
        self.__write_msg(init_message)

    def abort_snapshot(self, snapshot_id: UUID) -> None:
        abort_message = SnapshotAbort(
            snapshot_id=SnapshotId(str(snapshot_id)),
        )
        self.__write_msg(abort_message)
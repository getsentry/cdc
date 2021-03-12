from cdc.streams.backends import ProducerBackend
from cdc.streams.types import StreamMessage
import json  # type: ignore
from uuid import uuid1

from typing import Callable, List

from cdc.snapshots.control_protocol import SnapshotAbort, SnapshotInit
from cdc.sources.types import Payload
from cdc.streams.producer import Producer as StreamProducer
from cdc.snapshots.snapshot_control import SnapshotControl


class DummyProducerBackend(ProducerBackend):
    def __init__(self) -> None:
        self.items: List[bytes] = []

    def __len__(self) -> int:
        return len(self.items)

    def write(self, payload: StreamMessage, callback: Callable[[], None]) -> None:
        self.items.append(payload.payload)
        callback()

    def poll(self, timeout: float) -> None:
        pass

    def flush(self, timeout: float) -> int:
        pass


class TestSnapshotControl:
    def test_init(self) -> None:
        uuid = uuid1()
        backend = DummyProducerBackend()
        producer = StreamProducer(backend)
        control = SnapshotControl(producer, {})
        control.init_snapshot(snapshot_id=uuid, tables=["my_table"], product="snuba")

        reloaded = json.loads(backend.items[0])
        assert reloaded == {
            "snapshot-id": str(uuid),
            "product": "snuba",
            "tables": ["my_table"],
            "event": "snapshot-init",
        }

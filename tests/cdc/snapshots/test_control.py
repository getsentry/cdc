import json  # type: ignore
from uuid import uuid1

from typing import Callable, List

from cdc.snapshots.control_protocol import SnapshotAbort, SnapshotInit
from cdc.sources.types import Payload
from cdc.streams import Producer as StreamProducer
from cdc.snapshots.snapshot_control import SnapshotControl


class DummyProducer(StreamProducer):
    def __init__(self) -> None:
        self.items: List[bytes] = []

    def write(self, payload: Payload, callback: Callable[[], None]) -> None:
        self.items.append(payload)
        callback()

    def poll(self, timeout: float) -> None:
        pass


class TestSnapshotControl:
    def test_init(self) -> None:
        uuid = uuid1()
        producer = DummyProducer()
        control = SnapshotControl(producer, {})
        control.init_snapshot(snapshot_id=uuid, tables=["my_table"], product="snuba")

        reloaded = json.loads(producer.items[0])
        assert reloaded == {
            "snapshot-id": str(uuid),
            "product": "snuba",
            "tables": ["my_table"],
            "event": "snapshot-init",
        }

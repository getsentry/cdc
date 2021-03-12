from abc import ABC, abstractmethod
from cdc.types import Payload
from cdc.streams.types import StreamMessage
from dataclasses import dataclass, asdict
from typing import Any, Mapping, Sequence
import json  # type: ignore

from cdc.snapshots.snapshot_types import Xid, SnapshotId, SnapshotDescriptor


class ControlMessage(ABC):
    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError

    def to_stream(self) -> StreamMessage:
        json_string = json.dumps(self.to_dict())
        return StreamMessage(payload=Payload(json_string.encode("utf-8")))


@dataclass(frozen=True)
class SnapshotInit(ControlMessage):
    snapshot_id: SnapshotId
    product: str
    tables: Sequence[str]

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "event": "snapshot-init",
            "tables": self.tables,
            "snapshot-id": self.snapshot_id,
            "product": self.product,
        }


@dataclass(frozen=True)
class SnapshotAbort(ControlMessage):
    snapshot_id: SnapshotId

    def to_dict(self) -> Mapping[str, Any]:
        return {"event": "snapshot-abort", "snapshot-id": self.snapshot_id}


@dataclass(frozen=True)
class SnapshotLoaded(ControlMessage):
    snapshot_id: SnapshotId
    transaction_info: SnapshotDescriptor

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "event": "snapshot-loaded",
            "snapshot-id": self.snapshot_id,
            "transaction-info": asdict(self.transaction_info),
        }

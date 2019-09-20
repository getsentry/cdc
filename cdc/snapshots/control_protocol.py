from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import Any, Mapping, Sequence

from cdc.snapshots.snapshot_types import (
    Xid,
    SnapshotId,
    SnapshotDescriptor
)

class ControlMessage(ABC):
    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError


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
            "product": self.product
        }

@dataclass(frozen=True)
class SnapshotAbort(ControlMessage):
    snapshot_id: SnapshotId

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "event": "snapshot-abort",
            "snapshot-id": self.snapshot_id,
        }

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

from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import Any, Mapping

from cdc.snapshots.snapshot_types import (
    Xid,
    SnapshotId,
    SnapshotDescriptor
)

class ControlMessage(ABC):
    @abstractmethod
    def serialize(self) -> Mapping[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class SnapshotInit:
    snapshot_id: SnapshotId
    product: str

    def serialize(self) -> Mapping[str, Any]:
        return {
            "event": "snapshot-init",
            "snapshot-id": self.snapshot_id,
            "product": self.product
        }

@dataclass(frozen=True)
class SnapshotAbort:
    snapshot_id: SnapshotId

    def serialize(self) -> Mapping[str, Any]:
        return {
            "event": "snapshot-abort",
            "snapshot-id": self.snapshot_id,
        }

@dataclass(frozen=True)
class SnapshotLoaded:
    snapshot_id: SnapshotId
    datasets: Mapping[str, Mapping[str, Any]]
    transaction_info: SnapshotDescriptor

    def serialize(self) -> Mapping[str, Any]:
        return {
            "event": "snapshot-loaded",
            "snapshot-id": self.snapshot_id,
            "datasets": self.datasets,
            "transaction-info": asdict(self.transaction_info),
        }

from dataclasses import dataclass
from typing import NewType, Sequence

Xid = NewType("Xid", int)

SnapshotId = NewType("SnapshotId", str)

@dataclass(frozen=True)
class SnapshotDescriptor:
    """
    Represents a database snapshot
    TODO: Make it less postgres specific
    """
    xmin: Xid
    xmax: Xid
    xip_list: Sequence[Xid]

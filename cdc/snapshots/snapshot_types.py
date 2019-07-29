from dataclasses import dataclass
from typing import NewType, Sequence

Xid = NewType("Xid", int)

@dataclass(frozen=True)
class SnapshotDescriptor:
    """
    Represents a database snapshot
    TODO: Make it less postgres specific
    """
    id: str
    xmin: Xid
    xmax: Xid
    xip_list: Sequence[Xid]

from dataclasses import dataclass
from typing import NewType, Sequence

from uuid import UUID

Xid = NewType("Xid", int)

@dataclass(frozen=True)
class SnapshotDescriptor:
    """
    Represents a database snapshot
    TODO: Make it less postgres specific
    """
    id: UUID
    xmin: Xid
    xmax: Xid
    xip_list: Sequence[Xid]

from dataclasses import dataclass
from mypy_extensions import TypedDict
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

TablesConfig = TypedDict(
    'TablesConfig',
    {'table': str, 'columns': Sequence[str]}
)

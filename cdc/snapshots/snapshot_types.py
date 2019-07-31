from dataclasses import dataclass
from enum import Enum
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

class DumpState(Enum):
    WAIT_METADATA = 1
    WAIT_TABLE = 2
    WRITE_TABLE = 3
    ERROR = 4

from dataclasses import dataclass
from enum import Enum
from typing import NewType, Optional, Sequence

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

@dataclass(frozen=True)
class TableConfig:
    """
    Represents the snapshot configuration for a table.
    """
    table: str
    columns: Optional[Sequence[str]]

class DumpState(Enum):
    WAIT_METADATA = 1
    WAIT_TABLE = 2
    WRITE_TABLE = 3
    CLOSE = 4
    ERROR = 5

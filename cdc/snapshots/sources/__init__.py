from abc import ABC, abstractmethod
from typing import AnyStr, IO, List

from cdc.utils.registry import Registry
from cdc.snapshots.destinations import SnapshotDestination

class SnapshotDescriptor:
    """
    Represents a database snapshot
    TODO: Make it less postgres specific
    """
    
    def __init__(self, id: str, xmin: str, xmax: str, xip_list: List[str]):
        self.id = id
        self.xmin = xmin
        self.xmax = xmax
        self.xip_list = xip_list

    def contains(self, xid: str) -> bool:
        pass

    def __repr__(self) -> str:
        return "CDC Snapshot id: %s xmin: %s xmax: %s xip list: %s" % (
            self.id,
            self.xmin,
            self.xmax,
            self.xip_list,
        )


class SnapshotSource(ABC):
    """
    Takes a snapshot from the source database and store the content into
    the output object. 
    """

    @abstractmethod
    def dump(self,
        output: SnapshotDestination,
        tables: List[str],
    ) -> SnapshotDescriptor:
        raise NotImplementedError


from cdc.snapshots.sources.postgres_snapshot import postgres_snapshot_factory

registry: Registry[SnapshotSource] = Registry(
    {"postgres_logical": postgres_snapshot_factory}
)

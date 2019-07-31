from abc import ABC, abstractmethod

from typing import Generator, IO, Sequence, Optional

from cdc.snapshots.snapshot_types import (
    SnapshotDescriptor,
    SnapshotId,
    TablesConfig,
    DumpState,
)

from cdc.utils.registry import Registry

class SnapshotDestinationStorage(ABC):
    """
    Abstract the storage system used by the SnapshotDestination.
    """

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def set_metadata(self,
        tables: Sequence[TablesConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        raise NotImplementedError            

    @abstractmethod
    def get_table_file(self, table_name:str) -> IO[bytes]:
        """
        Returns the open file we will use to dump the table
        """
        raise NotImplementedError

    @abstractmethod
    def table_complete(self, table_file: IO[bytes]) -> None:
        """
        Signals we are done with this table
        """
        raise NotImplementedError

    @abstractmethod
    def close(self, state: DumpState) -> None:
        raise NotImplementedError

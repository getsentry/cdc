from abc import ABC, abstractmethod

from contextlib import contextmanager
from typing import Generator, IO, Sequence, Optional

from cdc.snapshots.snapshot_types import (
    SnapshotDescriptor,
    SnapshotId,
    TableConfig,
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
    def write_metadata(self,
        tables: Sequence[TableConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        raise NotImplementedError            

    @abstractmethod
    @contextmanager
    def get_table_file(
        self,
        table_name:str,
    ) -> Generator[IO[bytes], None, None]:
        """
        Returns the open file we will use to dump the table
        """
        raise NotImplementedError

    @abstractmethod
    def close(self, state: DumpState) -> None:
        raise NotImplementedError

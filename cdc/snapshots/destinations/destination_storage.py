from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from typing import IO, Generator, Optional, Sequence

from cdc.snapshots.snapshot_types import (
    DumpState,
    SnapshotDescriptor,
    SnapshotId,
    TableConfig,
    TableDumpFormat,
)
from cdc.utils.registry import Registry


class FileMode(Enum):
    READ = "read"
    WRITE = "write"


class SnapshotDestinationStorage(ABC):
    """
    Abstract the storage system used by the SnapshotDestination.
    """

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def write_metadata(
        self, tables: Sequence[TableConfig], snapshot: SnapshotDescriptor
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    @contextmanager
    def get_table_file(
        self,
        table_name: str,
        dump_format: TableDumpFormat,
        mode: FileMode,
        zip: bool = False,
    ) -> Generator[IO[bytes], None, None]:
        """
        Returns the open file we will use to dump the table
        """
        raise NotImplementedError

    @abstractmethod
    def close(self, state: DumpState) -> None:
        raise NotImplementedError

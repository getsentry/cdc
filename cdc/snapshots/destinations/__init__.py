from __future__ import annotations

import logging

from abc import ABC, abstractmethod
from typing import Generator, IO, Sequence, Optional

from contextlib import contextmanager, closing

from cdc.snapshots.snapshot_types import (
    SnapshotDescriptor,
    SnapshotId,
    TableConfig,
    TableDumpFormat,
    DumpState,
)
from cdc.snapshots.destinations.destination_storage import SnapshotDestinationStorage
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Registry

logger = LoggerAdapter(logging.getLogger(__name__))

class DestinationContext(ABC):
    """
    Manages the lifecycle of the SnapshotDestination.
    There are two contexts involved. An external one that manages the entire
    snapshot and an internal one per table.
    """
    
    @contextmanager
    def open(
        self,
        snapshot_id: SnapshotId,
        product: str,
    ) -> Generator[SnapshotDestination, None, None] :
        """
        Represents the snapshot context. This method handles the initialization
        of the snapshot and produces a SnapshotDestination object.
        """
        storage = self._open_storage(snapshot_id, product)
        with closing(SnapshotDestination(storage)) as destination:
            yield destination


    @abstractmethod
    def _open_storage(
        self,
        snapshot_id: SnapshotId,
        product: str,
    ) -> SnapshotDestinationStorage:
        """
        Initializes the SnapshotDestination object.
        """
        raise NotImplementedError


class SnapshotDestination:
    """
    Abstracts the destination of our snapshot. It is able to open/close
    the snapshot, allows the caller to provide metadata and it enforces
    the snapshot is created in a consistent way managing the state.

    This handles a context for each table exposing a file like object
    for them.
    """

    def __init__(self, storage: SnapshotDestinationStorage) -> None:
        self.__state = DumpState.WAIT_METADATA
        self.__storage = storage

    def get_name(self) -> str:
        return self.__storage.get_name()

    def write_metadata(self,
        tables: Sequence[TableConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        """
        Provides some metadata regarding the snapshot.
        The implementation decides what to do with that.
        """
        assert self.__state == DumpState.WAIT_METADATA, \
            "Cannot write metadata in the current state: %s" % self.__state
        self.__storage.write_metadata(tables, snapshot)
        self.__state = DumpState.WAIT_TABLE

    @contextmanager
    def open_table(
        self,
        table_name:str,
        dump_format: TableDumpFormat,
    ) -> Generator[IO[bytes], None, None] :
        """
        Call this to provide the table schema and start the dump
        of one table
        """
        assert self.__state == DumpState.WAIT_TABLE, \
            "Cannot write table header in the current state %s" % self.__state
        try:
            self.__state = DumpState.WRITE_TABLE
            with self.__storage.get_table_file(table_name, dump_format) as table_file:
                logger.debug("Opening table file for %s", table_name)
                yield table_file
                self.__state = DumpState.WAIT_TABLE
        except:
            self.__state = DumpState.ERROR
            raise
            
    def close(self) -> None:
        """
        Closes the snapshot. This is called by the context manager.
        """
        assert self.__state == DumpState.WAIT_TABLE, \
            "Cannot close a snapshot in the current state: %s" % self.__state
        self.__storage.close(self.__state)
        self.__state = DumpState.CLOSE


from cdc.snapshots.destinations.file_snapshot import directory_destination_factory

registry: Registry[DestinationContext] = Registry(
    {"directory": directory_destination_factory}
)

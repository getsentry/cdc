from __future__ import annotations

import logging

from abc import ABC, abstractmethod
from typing import Generator, IO, Sequence, Optional

from contextlib import contextmanager, closing

from cdc.snapshots.snapshot_types import (
    SnapshotDescriptor,
    SnapshotId,
    TablesConfig,
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
    def open_snapshot(
        self,
        snapshot_id: SnapshotId,
        product: str,
    ) -> Generator[SnapshotDestination, None, None] :
        """
        Represents the snapshot context. This method handles the initialization
        of the snapshot and produces a SnapshotDestination object.
        """
        with closing(self._open_snapshot_impl(snapshot_id, product)) as snapshot:
            yield snapshot

    @abstractmethod
    def _open_snapshot_impl(
        self,
        snapshot_id: SnapshotId,
        product: str,
    ) -> SnapshotDestination:
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

    def set_metadata(self,
        tables: Sequence[TablesConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        """
        Provides some metadata regarding the snapshot.
        The implementation decides what to do with that.
        """
        assert self.__state == DumpState.WAIT_METADATA, \
            "Cannot write metadata in the current state: %s" % self.__state
        self.__storage.set_metadata(tables, snapshot)
        self.__state = DumpState.WAIT_TABLE

    @contextmanager
    def open_table(self, table_name:str) -> Generator[IO[bytes], None, None] :
        """
        Call this to provide the table schema and start the dump
        of one table
        """
        assert self.__state == DumpState.WAIT_TABLE, \
            "Cannot write table header in the current state %s" % self.__state
        try:
            self.__state = DumpState.WRITE_TABLE
            table_file = self.__storage.get_table_file(table_name)
            logger.debug("Opening table file for %s", table_name)
            yield table_file
        except:
            self.__state = DumpState.ERROR
            raise
        else:
            self.__state = DumpState.WAIT_TABLE
        finally:
            self.__storage.table_complete(table_file)
            
    def close(self) -> None:
        """
        Closes the snapshot. This is called by the context manager.
        """
        self.__storage.close(self.__state)


from cdc.snapshots.destinations.file_snapshot import directory_destination_factory

registry: Registry[DestinationContext] = Registry(
    {"directory": directory_destination_factory}
)

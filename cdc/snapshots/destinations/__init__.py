from __future__ import annotations

import logging

from abc import ABC, abstractmethod
from enum import Enum
from typing import Generator, IO, Sequence, Optional

from contextlib import contextmanager

from cdc.snapshots.snapshot_types import SnapshotDescriptor, SnapshotId, TablesConfig
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Registry

logger = LoggerAdapter(logging.getLogger(__name__))

class DumpState(Enum):
    WAIT_METADATA = 1
    WAIT_TABLE = 2
    WRITE_TABLE = 3
    ERROR = 4


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
        try:
            snapshot = self._open_snapshot_impl(snapshot_id, product)
            yield snapshot
        finally:
            snapshot.close()

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


class SnapshotDestination(ABC):
    """
    Abstracts the destination of our snapshot. It is able to open/close
    the snapshot, allows the caller to provide metadata and it enforces
    the snapshot is created in a consistent way managing the state.

    This handles a context for each table exposing a file like object
    for them.
    """

    def __init__(self, snapshot_id: SnapshotId, product:str) -> None:
        self.__state = DumpState.WAIT_METADATA
        self.product = product
        self.id = snapshot_id

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

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
        self._set_metadata_impl(tables, snapshot)
        self.__state = DumpState.WAIT_TABLE

    @abstractmethod
    def _set_metadata_impl(self,
        tables: Sequence[TablesConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        raise NotImplementedError

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
            table_file = self._get_table_file(table_name)
            logger.debug("Opening table file for %s", table_name)
            yield table_file
        except:
            self.__state = DumpState.ERROR
            raise
        else:
            self.__state = DumpState.WAIT_TABLE
        finally:
            self._table_complete(table_file)
            

    @abstractmethod
    def _get_table_file(self, table_name:str) -> IO[bytes]:
        """
        Returns the open file we will use to dump the table
        """
        raise NotImplementedError

    @abstractmethod
    def _table_complete(self, table_file: IO[bytes]) -> None:
        """
        Signals we are done with this table
        """
        raise NotImplementedError

    def close(self) -> None:
        """
        Closes the snapshot. This is called by the context manager.
        """
        self._close_impl(self.__state)

    @abstractmethod
    def _close_impl(self, state: DumpState) -> None:
        raise NotImplementedError


from cdc.snapshots.destinations.file_snapshot import directory_dump_factory

registry: Registry[DestinationContext] = Registry(
    {"directory": directory_dump_factory}
)

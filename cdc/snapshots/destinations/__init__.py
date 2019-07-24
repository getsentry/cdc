from abc import ABC, abstractmethod
from enum import Enum
from typing import IO, List, Optional

from cdc.utils.registry import Registry

class DumpState(Enum):
    WAIT_METADATA = 1
    WAIT_TABLE = 2
    WRITE_TABLE = 4


class SnapshotDestination(ABC):
    """
    Abstracts the destination of our snapshot.
    The destination wraps a file abstraction enriched with some
    methods to write metadata. This does not have to be a file
    on the file system.
    """

    def __init__(self) -> None:
        self.__state = DumpState.WAIT_METADATA
        self.__table: Optional[str] = None

    @abstractmethod
    def get_stream(self) -> IO[bytes]:
        """
        Return a file abstraction where the content of the dump should be written
        """
        raise NotImplementedError

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    def set_metadata(self,
        tables: List[str],
        snapshot_id: str,
    ) -> None:
        """
        Provides some metadata regarding the snapshot.
        The implementation decides what to do with that.
        """
        assert self.__state == DumpState.WAIT_METADATA, \
            "Cannot write metadata in the current state: %s" % self.__state
        self._set_metadata_impl(tables, snapshot_id)
        self.__state = DumpState.WAIT_TABLE

    @abstractmethod
    def _set_metadata_impl(self,
        tables: List[str],
        snapshot_id: str,
    ) -> None:
        raise NotImplementedError

    def start_table(self, table_name:str) -> None:
        """
        Call this to provide the table schema and start the dump
        of one table
        """
        assert self.__state == DumpState.WAIT_TABLE, \
            "Cannot write table header in the current state %s" % self.__state
        self.__table = table_name
        self._start_table_impl(table_name)
        self.__state = DumpState.WRITE_TABLE

    def _start_table_impl(self, table_name:str) -> None:
        raise NotImplementedError

    def end_table(self) -> None:
        """
        Call this to write the footer of the table
        """
        assert self.__state == DumpState.WRITE_TABLE, \
            "Cannot write table footer in the current state %s" % self.__state
        assert self.__table is not None
        self._end_table_impl(self.__table)
        self.__table = None
        self.__state = DumpState.WAIT_TABLE
        
    def _end_table_impl(self, table: str) -> None:
        raise NotImplementedError

class DestinationContext(ABC):
    """
    Manages the lifecycle of the SnapshotDestination
    """
    
    @abstractmethod
    def __enter__(self) -> SnapshotDestination:
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, type, value, tb) -> None:
        raise NotImplementedError


from cdc.snapshots.destinations.file_snapshot import file_dump_factory

registry: Registry[DestinationContext] = Registry(
    {"file": file_dump_factory}
)

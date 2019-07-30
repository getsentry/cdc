import jsonschema  # type: ignore
import logging
import tempfile

from abc import ABC, abstractmethod
from os import mkdir, path
from typing import AnyStr, Generator, IO, Optional, Sequence

from cdc.snapshots.destinations import DestinationContext, DumpState, SnapshotDestination
from cdc.snapshots.snapshot_types import SnapshotDescriptor, SnapshotId
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


class DirectorySnapshot(SnapshotDestination):
    """
    Snapshot based on directories.

    It creates a main directory based on the snapshot id.
    It creates a  file per table and a metadata file.
    When it is closed in a consistent state it adds an empty "compelete" file
    to mark the snapshot is valid.
    """

    def __init__(self, snapshot_id: SnapshotId, directory_name: str) -> None:
        super(DirectorySnapshot, self).__init__(snapshot_id)
        self.__directory_name = directory_name

    def get_name(self) -> str:
        return  self.__directory_name

    def _set_metadata_impl(self,
        tables: Sequence[str],
        snapshot: SnapshotDescriptor,
    ) -> None:
        meta_file_name = path.join(self.__directory_name, "metadata.txt")
        with open(meta_file_name, "w") as meta_file:
            meta_file.write(("CDC Snapshot: %r \n" % self.id))
            meta_file.write(("Transactions: %r \n" % snapshot))
            meta_file.write(("Tables: %s \n" % ", ".join(tables)))
    
    def _get_table_file(self, table_name:str) -> IO[bytes]:
        file_name = path.join(
            self.__directory_name,
            "table_%s" % table_name,
        )
        return open(file_name, "wb")

    def _table_complete(self, table_file: IO[bytes]) -> None:
        table_file.close()

    def _close_impl(self, state: DumpState) -> None:
        if state != DumpState.ERROR:
            complete_file_name = path.join(self.__directory_name, "complete")
            with open(complete_file_name, "w"):
                pass

class DirectoryDestinationContext(DestinationContext):
    
    def __init__(self, directory_name: str) -> None:
        self.__directory_name = directory_name

    def _open_snapshot_impl(self, snapshot_id: SnapshotId) -> DirectorySnapshot:
        dir_name = path.join(
            self.__directory_name,
            'cdc_snapshot_%s' % snapshot_id,
        )
        mkdir(dir_name)
        logger.debug("Snapshot directory created %s", dir_name)
        return DirectorySnapshot(snapshot_id, dir_name)


def directory_dump_factory(
    configuration: Configuration
) -> DirectoryDestinationContext:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {
                "location": {"type": "string"},
            },
            "required": ["location"],
        },
    )
    return DirectoryDestinationContext(
        directory_name=configuration["location"],
    )

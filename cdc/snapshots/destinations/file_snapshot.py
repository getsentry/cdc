from __future__ import annotations

import jsonschema  # type: ignore
import logging
import tempfile

from abc import ABC, abstractmethod
from os import mkdir, path
from typing import AnyStr, Generator, IO, Optional, Sequence

from cdc.snapshots.destinations import DestinationContext, SnapshotDestination
from cdc.snapshots.destinations.destination_storage import SnapshotDestinationStorage
from cdc.snapshots.snapshot_types import SnapshotDescriptor, SnapshotId, TablesConfig, DumpState
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


class DirectoryDestinationContext(DestinationContext):
    
    def __init__(self, directory_name: str) -> None:
        self.__directory_name = directory_name

    def _open_snapshot_impl(
        self,
        snapshot_id: SnapshotId,
        product: str,
    ) -> SnapshotDestination:
        dir_name = path.join(
            self.__directory_name,
            'cdc_snapshot_%s_%s' % (product, snapshot_id),
        )
        mkdir(dir_name)
        logger.debug("Snapshot directory created %s", dir_name)
        snapshot_storage = DirectorySnapshot(
            snapshot_id,
            product,
            dir_name,
        )
        return SnapshotDestination(snapshot_storage)


class DirectorySnapshot(SnapshotDestinationStorage):
    """
    Snapshot based on directories.

    It creates a main directory based on the snapshot id.
    It creates a  file per table and a metadata file.
    When it is closed in a consistent state it adds an empty "compelete" file
    to mark the snapshot is valid.
    """

    def __init__(self, snapshot_id: SnapshotId, product:str, directory_name: str) -> None:
        self.id = snapshot_id
        self.product = product
        self.__directory_name = directory_name

    def get_name(self) -> str:
        return  self.__directory_name

    def set_metadata(self,
        tables: Sequence[TablesConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        meta_file_name = path.join(self.__directory_name, "metadata.txt")
        with open(meta_file_name, "w") as meta_file:
            meta_file.write("CDC Snapshot: %r \n" % self.id)
            meta_file.write("Product: %s \n" % self.product)
            meta_file.write("Transactions: %r \n" % snapshot)
            meta_file.write("Tables:\n")
            for table in tables:
                meta_file.write("-%s - %s\n" % (
                    table['table'],
                    ','.join(table['columns']),
                ))
    
    def get_table_file(self, table_name:str) -> IO[bytes]:
        file_name = path.join(
            self.__directory_name,
            "table_%s" % table_name,
        )
        return open(file_name, "wb")

    def table_complete(self, table_file: IO[bytes]) -> None:
        table_file.close()

    def close(self, state: DumpState) -> None:
        if state != DumpState.ERROR:
            complete_file_name = path.join(self.__directory_name, "complete")
            with open(complete_file_name, "w"):
                pass


def directory_destination_factory(
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

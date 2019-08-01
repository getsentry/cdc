from __future__ import annotations

import jsonschema  # type: ignore
import json
import logging
import tempfile

from abc import ABC, abstractmethod
from contextlib import contextmanager, closing
from dataclasses import asdict
from datetime import datetime
from os import mkdir, path
from typing import AnyStr, Generator, IO, Optional, Sequence

from cdc.snapshots.destinations import DestinationContext, SnapshotDestination
from cdc.snapshots.destinations.destination_storage import SnapshotDestinationStorage
from cdc.snapshots.snapshot_types import (
    SnapshotDescriptor,
    SnapshotId,
    TableConfig,
    TableDumpFormat,
    DumpState,
)
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


class DirectoryDestinationContext(DestinationContext):
    
    def __init__(self, directory_name: str) -> None:
        self.__directory_name = directory_name

    def _open_storage(
        self,
        snapshot_id: SnapshotId,
        product: str,
    ) -> SnapshotDestinationStorage:
        dir_name = path.join(
            self.__directory_name,
            'cdc_snapshot_%s_%s' % (product, snapshot_id),
        )
        mkdir(dir_name)
        logger.debug("Snapshot directory created %s", dir_name)
        return DirectorySnapshot(
            snapshot_id,
            product,
            dir_name,
        )


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
        self.__tables_dir = path.join(self.__directory_name, "tables")

    def get_name(self) -> str:
        return  self.__directory_name

    def write_metadata(self,
        tables: Sequence[TableConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        mkdir(self.__tables_dir)
        meta_file_name = path.join(self.__directory_name, "metadata.json")
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        meta_content = {
            "snapshot_id": self.id,
            "product": self.product,
            "transactions": asdict(snapshot),
            "content": [asdict(t) for t in tables],
            "start_timestamp": timestamp,
        }
        with open(meta_file_name, "w") as meta_file:
            json.dump(meta_content, meta_file)
    
    @contextmanager
    def get_table_file(
        self,
        table_name: str,
        dump_format: TableDumpFormat,
    )-> Generator[IO[bytes], None, None]:
        extensions = {
            TableDumpFormat.CSV: 'csv',
            TableDumpFormat.BINARY: 'bin',
            TableDumpFormat.TEXT: '.txt',
        }
        
        file_name = path.join(
            self.__tables_dir,
            "table_%s.%s" % (table_name, extensions[dump_format])
        )
        with open(file_name, "wb") as table_file:
            yield table_file

    def close(self, state: DumpState) -> None:
        if state != DumpState.ERROR:
            complete_file_name = path.join(self.__directory_name, "complete.json")
            with open(complete_file_name, "w") as complete_file:
                now = datetime.now()
                timestamp = datetime.timestamp(now)
                json.dump(
                    {"finish_timestamp": timestamp},
                    complete_file,
                )


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

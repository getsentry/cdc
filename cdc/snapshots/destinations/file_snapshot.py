from __future__ import annotations

import gzip
import json
import logging
import tempfile
from abc import ABC, abstractmethod
from contextlib import closing, contextmanager
from dataclasses import asdict
from datetime import datetime
from enum import Enum
from os import mkdir, path
from typing import IO, Any, AnyStr, Generator, Optional, Sequence

import jsonschema  # type: ignore
from cdc.snapshots.destinations import DestinationContext, SnapshotDestination
from cdc.snapshots.destinations.destination_storage import (
    FileMode,
    SnapshotDestinationStorage,
)
from cdc.snapshots.snapshot_types import (
    DumpState,
    SnapshotDescriptor,
    SnapshotId,
    TableConfig,
    TableDumpFormat,
)
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


class DirectoryDestinationContext(DestinationContext):
    def __init__(self, directory_name: str) -> None:
        self.__directory_name = directory_name

    def _open_storage(
        self, snapshot_id: SnapshotId, product: str
    ) -> SnapshotDestinationStorage:
        dir_name = path.join(
            self.__directory_name, "cdc_snapshot_%s_%s" % (product, snapshot_id)
        )
        mkdir(dir_name)
        logger.debug("Snapshot directory created %s", dir_name)
        return DirectorySnapshot(snapshot_id, product, dir_name)


class DirectorySnapshot(SnapshotDestinationStorage):
    """
    Snapshot based on directories.

    It creates a main directory based on the snapshot id.
    It creates a  file per table and a metadata file.
    When it is closed in a consistent state it adds an empty "compelete" file
    to mark the snapshot is valid.
    """

    def __init__(
        self, snapshot_id: SnapshotId, product: str, directory_name: str
    ) -> None:
        self.id = snapshot_id
        self.product = product
        self.__directory_name = directory_name
        self.__tables_dir = path.join(self.__directory_name, "tables")

    def get_name(self) -> str:
        return self.__directory_name

    def write_metadata(
        self, tables: Sequence[TableConfig], snapshot: SnapshotDescriptor
    ) -> None:
        mkdir(self.__tables_dir)
        meta_file_name = path.join(self.__directory_name, "metadata.json")
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        meta_content = {
            "snapshot_id": self.id,
            "product": self.product,
            "transactions": asdict(snapshot),
            "content": [t.to_dict() for t in tables],
            "start_timestamp": timestamp,
        }

        def safe_dump_default(value: Any) -> Any:
            if isinstance(value, Enum):
                return value.value
            else:
                raise TypeError

        with open(meta_file_name, "w") as meta_file:
            json.dump(meta_content, meta_file, default=safe_dump_default)

    @contextmanager
    def get_table_file(
        self,
        table_name: str,
        dump_format: TableDumpFormat,
        mode: FileMode,
        zip: bool = False,
    ) -> Generator[IO[bytes], None, None]:
        extensions = {
            TableDumpFormat.CSV: "csv",
            TableDumpFormat.BINARY: "bin",
            TableDumpFormat.TEXT: "txt",
        }

        file_name = path.join(
            self.__tables_dir, "%s.%s" % (table_name, extensions[dump_format])
        )
        file_mode = "wb" if mode == FileMode.WRITE else "rb"
        if not zip:
            with open(file_name, file_mode) as table_file:
                yield table_file
        else:
            with gzip.open(f"{file_name}.gz", file_mode, compresslevel=5) as table_file:
                yield table_file

    def close(self, state: DumpState) -> None:
        if state != DumpState.ERROR:
            complete_file_name = path.join(self.__directory_name, "complete.json")
            with open(complete_file_name, "w") as complete_file:
                now = datetime.now()
                timestamp = datetime.timestamp(now)
                json.dump({"finish_timestamp": timestamp}, complete_file)


def directory_destination_factory(
    configuration: Configuration
) -> DirectoryDestinationContext:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {"location": {"type": "string"}},
            "required": ["location"],
        },
    )
    return DirectoryDestinationContext(directory_name=configuration["location"])

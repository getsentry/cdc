import jsonschema  # type: ignore
import logging
import tempfile

from abc import ABC, abstractmethod
from typing import AnyStr, IO, Sequence

from cdc.snapshots.destinations import DestinationContext, SnapshotDestination
from cdc.snapshots.snapshot_types import SnapshotDescriptor
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


class FileSnapshot(SnapshotDestination):
    def __init__(self, file: IO[bytes]) -> None:
        super(FileSnapshot, self).__init__()
        self.__file: IO[bytes] = file

    def get_stream(self) -> IO[bytes]:
        return  self.__file

    def get_name(self) -> str:
        return  self.__file.name

    def _set_metadata_impl(self,
        tables: Sequence[str],
        snapshot: SnapshotDescriptor,
    ) -> None:
        assert self.__file, "The output file is not open yet."
        self.__file.write(("# CDC Snapshot: %s \n" % snapshot.id).encode())
        self.__file.write(("# Tables: %s \n" % ", ".join(tables)).encode())
    
    def _start_table_impl(self, table_name:str) -> None:
        self.__file.write(("# Table %s\n" % table_name).encode())

    def _end_table_impl(self, table: str) -> None:
        self.__file.write(("# End table %s\n\n" % table).encode())


class FileDestinationContext(DestinationContext):
    
    def __init__(self, directory: str) -> None:
        self.__directory = directory

    def __enter__(self) -> SnapshotDestination:
        self.__file = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="cdc_snapshot_",
            delete=False,
            dir=self.__directory,
        )
        logger.debug("Snapshot file created %s", self.__file.name)
        return FileSnapshot(self.__file)

    def __exit__(self, type, value, tb) -> None:
        self.__file.close()


def file_dump_factory(
    configuration: Configuration
) -> FileDestinationContext:
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
    return FileDestinationContext(
        directory=configuration["location"],
    )

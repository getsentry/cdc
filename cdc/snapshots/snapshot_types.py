from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, NewType, Optional, Sequence

Xid = NewType("Xid", int)

SnapshotId = NewType("SnapshotId", str)


@dataclass(frozen=True)
class SnapshotDescriptor:
    """
    Represents a database snapshot
    TODO: Make it less postgres specific
    """

    xmin: Xid
    xmax: Xid
    xip_list: Sequence[Xid]


@dataclass(frozen=True)
class TableConfig:
    """
    Represents the snapshot configuration for a table.
    """

    table: str
    columns: Optional[Sequence[ColumnConfig]]

    @classmethod
    def from_dict(cls, content: Mapping[str, Any]) -> TableConfig:
        columns = []
        for column in content["columns"]:
            # This has already been validated by the jsonschema validator
            assert isinstance(column, Mapping)
            if "formatter" in column:
                formatter: Optional[FormatterConfig] = FormatterConfig.from_dict(
                    column["formatter"]
                )
            else:
                formatter = None
            columns.append(ColumnConfig(name=column["name"], formatter=formatter))
        return TableConfig(content["table"], columns)


class FormatterConfig(ABC):
    """
    Parent class to all the the formatter configs.
    """

    @classmethod
    def from_dict(cls, content: Mapping[str, str]) -> FormatterConfig:
        if content["type"] == "datetime":
            return DateTimeFormatterConfig.from_dict(content)
        else:
            raise ValueError("Unknown config for column formatter")


@dataclass(frozen=True)
class DateTimeFormatterConfig(FormatterConfig):
    format: str

    @classmethod
    def from_dict(cls, content: Mapping[str, str]) -> DateTimeFormatterConfig:
        return DateTimeFormatterConfig(content["format"])


@dataclass(frozen=True)
class ColumnConfig:
    """
    Represents a column in the snapshot configuration.
    """

    name: str
    formatter: Optional[FormatterConfig] = None


class DumpState(Enum):
    WAIT_METADATA = 1
    WAIT_TABLE = 2
    WRITE_TABLE = 3
    CLOSE = 4
    ERROR = 5


class TableDumpFormat(Enum):
    BINARY = 1
    TEXT = 2
    CSV = 3

from __future__ import annotations

from abc import ABC, abstractmethod
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
    zip: bool
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
        return TableConfig(content["table"], content["zip"], columns)

    def to_dict(self) -> Mapping[str, Any]:
        base = {"table": self.table, "zip": self.zip}
        return (
            base
            if self.columns is None
            else {**base, "columns": [c.to_dict() for c in self.columns]}
        )


class FormatterConfig(ABC):
    """
    Parent class to all the the formatter configs.
    """

    @abstractmethod
    def to_dict(self) -> Mapping[str, str]:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, content: Mapping[str, str]) -> FormatterConfig:
        if content["type"] == "datetime":
            return DateTimeFormatterConfig.from_dict(content)
        else:
            raise ValueError("Unknown config for column formatter")


class DateFormatPrecision(Enum):
    SECOND = "second"
    # Add more if/when needed


@dataclass(frozen=True)
class DateTimeFormatterConfig(FormatterConfig):
    precision: DateFormatPrecision

    @classmethod
    def from_dict(cls, content: Mapping[str, str]) -> DateTimeFormatterConfig:
        return DateTimeFormatterConfig(DateFormatPrecision(content["precision"]))

    def to_dict(self) -> Mapping[str, str]:
        return {"type": "datetime", "precision": self.precision.value}


@dataclass(frozen=True)
class ColumnConfig:
    """
    Represents a column in the snapshot configuration.
    """

    name: str
    formatter: Optional[FormatterConfig] = None

    def to_dict(self) -> Mapping[str, Any]:
        return (
            {"name": self.name}
            if self.formatter is None
            else {"name": self.name, "formatter": self.formatter.to_dict()}
        )


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

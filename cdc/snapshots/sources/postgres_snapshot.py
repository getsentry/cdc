from abc import ABC, abstractmethod
import re
from psycopg2 import (  # type: ignore
    connect,
    sql,
)
from typing import Any, AnyStr, cast, IO, Mapping, Sequence

import jsonschema  # type: ignore
import logging
import uuid

from cdc.snapshots.sources import SnapshotSource
from cdc.snapshots.snapshot_types import (
    ColumnConfig,
    DateTimeFormatterConfig,
    SnapshotDescriptor,
    TableConfig,
    TableDumpFormat,
    Xid,
)
from cdc.snapshots.destinations import SnapshotDestination
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


class PostgresSnapshot(SnapshotSource):
    def __init__(self, dsn: str) -> None:
        self.__dsn = dsn

    def dump(
        self, output: SnapshotDestination, tables: Sequence[TableConfig]
    ) -> SnapshotDescriptor:
        logger.debug("Establishing replication connection to %r...", self.__dsn)
        with connect(self.__dsn).cursor() as cursor:
            cursor.execute(
                """
                SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE
                """
            )
            cursor.execute("SELECT txid_current_snapshot()")
            current_snapshot = cursor.fetchone()
            xmin, xmax, xip_list = current_snapshot[0].split(":")
            xip_list = [Xid(int(xid)) for xid in xip_list.split(",") if xid]
            snapshot_descriptor = SnapshotDescriptor(
                Xid(int(xmin)), Xid(int(xmax)), xip_list
            )

            logger.debug("Snapshot exported %r.", snapshot_descriptor)

            output.write_metadata(tables=tables, snapshot=snapshot_descriptor)

            for table in tables:
                table_name = table.table
                with output.open_table(table_name, TableDumpFormat.CSV) as table_file:
                    logger.debug(
                        "Dumping table %s, using snapshot: %r...",
                        table,
                        snapshot_descriptor,
                    )

                    if not table.columns:
                        cols_expr = sql.SQL("*")
                    else:
                        cols_expr = sql.SQL(", ").join(
                            [format_column(column) for column in table.columns]
                        )

                    cursor.copy_expert(
                        sql.SQL(
                            "COPY (SELECT {columns} FROM {table}) TO STDOUT WITH CSV HEADER"
                        ).format(table=sql.Identifier(table_name), columns=cols_expr),
                        table_file,
                    )

                logger.info("Dumped %s rows from %r.", cursor.rowcount, table)
        return snapshot_descriptor


def format_column(column: ColumnConfig) -> sql.SQL:
    if column.formatter is None:
        return sql.Identifier(column.name)
    elif isinstance(column.formatter, DateTimeFormatterConfig):
        return format_datetime(column.name, column.formatter)
    else:
        raise ValueError(f"Unknown formatter type {type(column.formatter)}")


# We could add more mappings if needed, which is unlikely.
DATETIME_MAPPING = {
    re.escape("%Y"): "YYYY",
    re.escape("%m"): "MM",
    re.escape("%d"): "DD",
    re.escape("%H"): "HH24",
    re.escape("%M"): "MI",
    re.escape("%S"): "SS",
}

DATETIME_REGEX = re.compile("|".join(DATETIME_MAPPING.keys()))


def format_datetime(col_name: str, formatter: DateTimeFormatterConfig) -> sql.SQL:
    postgres_format = DATETIME_REGEX.sub(
        lambda match: DATETIME_MAPPING[match.group(0)], formatter.format
    )
    return sql.SQL("to_char({column}, {format}) AS {alias}").format(
        column=sql.Identifier(col_name),
        format=sql.Literal(postgres_format),
        alias=sql.Identifier(col_name),
    )


def postgres_snapshot_factory(configuration: Configuration) -> PostgresSnapshot:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {"dsn": {"type": "string"}},
            "required": ["dsn"],
        },
    )
    return PostgresSnapshot(dsn=configuration["dsn"])

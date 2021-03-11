from abc import ABC, abstractmethod
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
                            [sql.Identifier(column.name) for column in table.columns]
                        )

                    cursor.copy_expert(
                        sql.SQL(
                            "COPY (SELECT {columns} FROM {table}) TO STDOUT WITH CSV HEADER"
                        ).format(table=sql.Identifier(table_name), columns=cols_expr),
                        table_file,
                    )

                logger.info("Dumped %s rows from %r.", cursor.rowcount, table)
        return snapshot_descriptor


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

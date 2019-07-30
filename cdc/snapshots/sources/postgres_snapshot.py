from abc import ABC, abstractmethod
from psycopg2 import ( # type: ignore
    connect,
    sql,
)
from typing import AnyStr, IO, Sequence

import jsonschema  # type: ignore
import logging
import uuid

from cdc.snapshots.sources import SnapshotSource
from cdc.snapshots.snapshot_types import SnapshotDescriptor
from cdc.snapshots.destinations import SnapshotDestination
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))

class PostgresSnapshot(SnapshotSource):
    def __init__(self, dsn: str) -> None:
        self.__dsn = dsn
    
    def dump(self, output: SnapshotDestination, tables: Sequence[str]) -> SnapshotDescriptor:
        assert len(tables) == 1, "We do not support multiple tables just yet."
        
        logger.debug("Establishing replication connection to %r...", self.__dsn)
        with connect(self.__dsn).cursor() as cursor:
            cursor.execute("""
                SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE
                """)
            cursor.execute("SELECT txid_current_snapshot()")
            current_snapshot = cursor.fetchone()
            xmin, xmax, xip_list = current_snapshot[0].split(':')

            
            snapshot_descriptor = SnapshotDescriptor(
                xmin,
                xmax,
                xip_list,
            )
            
            logger.debug('Snapshot exported %r.', snapshot_descriptor)

            output.set_metadata(
                tables=tables,
                snapshot=snapshot_descriptor
            )

            with output.open_table(tables[0]) as table_file:
                logger.debug(
                    'Dumping table %s using snapshot: %r...',
                    tables[0],
                    snapshot_descriptor,
                )
                
                cursor.copy_expert(
                    sql.SQL(
                        "COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER"
                    ).format(
                        table=sql.Identifier(tables[0]),
                    ),
                    table_file,
                )

            logger.debug('Dumped %s rows from %r.', cursor.rowcount, tables[0])
        return snapshot_descriptor


def postgres_snapshot_factory(
    configuration: Configuration
) -> PostgresSnapshot:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {
                "dsn": {"type": "string"},
            },
            "required": ["dsn"],
        },
    )
    return PostgresSnapshot(
        dsn=configuration["dsn"],
    )

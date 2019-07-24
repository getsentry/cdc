from abc import ABC, abstractmethod
from psycopg2 import ( # type: ignore
    connect,
    sql,
)
from typing import AnyStr, IO, List

import jsonschema  # type: ignore
import logging

from cdc.snapshots.sources import SnapshotSource, SnapshotDescriptor
from cdc.snapshots.destinations import SnapshotDestination
from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))

class PostgresSnapshot(SnapshotSource):
    def __init__(self, dsn: str) -> None:
        self.__dsn = dsn
    
    def dump(self, output: SnapshotDestination, tables: List[str]) -> SnapshotDescriptor:
        assert len(tables) == 1, "We do not support multiple tablesd just yet."
        
        logger.debug("Establishing replication connection to %r...", self.__dsn)
        with connect(self.__dsn).cursor() as cursor:
            cursor.execute("""
                SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE
                """)
            cursor.execute("SELECT txid_current_snapshot(), pg_export_snapshot()")
            current_snapshot, snapshot_identifier = cursor.fetchone()
            xmin, xmax, xip_list = current_snapshot.split(':')

            snapshot_descriptor = SnapshotDescriptor(
                snapshot_identifier,
                xmin,
                xmax,
                xip_list,
            )
            
            logger.debug('Snapshot exported %s.' % snapshot_descriptor)

            output.set_metadata(
                tables=tables,
                snapshot_id=repr(snapshot_descriptor)
            )

            logger.debug('Dumping data using snapshot: %s...', snapshot_descriptor.id)
            output.start_table(tables[0])
            
            cursor.copy_expert(
                sql.SQL(
                    "COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER"
                ).format(
                    table=sql.Identifier(tables[0]),
                ),
                output.get_stream(),
            )

            output.end_table()
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
                "slot": {"type": "object"},
            },
            "required": ["dsn"],
        },
    )
    return PostgresSnapshot(
        dsn=configuration["dsn"],
    )

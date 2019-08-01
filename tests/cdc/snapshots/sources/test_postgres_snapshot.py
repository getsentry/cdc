import pytest
import psycopg2
import uuid

from io import StringIO

from contextlib import closing, contextmanager
from typing import AnyStr, Generator, IO, Sequence
from unittest.mock import MagicMock

from cdc.snapshots.sources.postgres_snapshot import PostgresSnapshot
from cdc.snapshots.destinations import SnapshotDestination, DumpState
from cdc.snapshots.destinations.destination_storage import SnapshotDestinationStorage
from cdc.snapshots.snapshot_types import SnapshotDescriptor, SnapshotId, TableConfig, TableDumpFormat
from cdc.testutils.fixtures import dsn

class FakeDestination(SnapshotDestinationStorage):
    def __init__(self, snapshot_id: SnapshotId) -> None:
        self.stream = StringIO()
        self.id = snapshot_id

    def get_name(self) -> str:
        raise NotImplementedError

    def write_metadata(self,
        tables: Sequence[TableConfig],
        snapshot: SnapshotDescriptor,
    ) -> None:
        self.stream.write("META %s %s\n" % (tables, self.id))

    @contextmanager
    def get_table_file(
        self,
        table_name:str,
        dump_format: TableDumpFormat,
    ) -> Generator[IO[bytes], None, None]:
        self.stream.write("START %s\n" % table_name)
        yield self.stream
        self.stream.write("END TABLE\n")

    def close(self, state: DumpState) -> None:
        self.stream.write("SNAPSHOT OVER\n")

def test_snapshot(dsn):
    with closing(psycopg2.connect(dsn)) as connection:
        connection.autocommit = False

        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE test_snapshot (a integer, b text, c timestamptz, primary key(a))"
            )
            cursor.execute(
                # Basic data
                "INSERT INTO test_snapshot (a, b, c) VALUES (%s, %s, %s)",
                [1, 'test', '2019-06-16 06:21:39+00']
            )
            cursor.execute(
                # NULL values
                "INSERT INTO test_snapshot (a, b) VALUES (%s, %s)",
                [2, 'test'],
            )
            cursor.execute(
                # empty string
                "INSERT INTO test_snapshot (a, b) VALUES (%s, %s)",
                [3, ''],
            )
            cursor.execute(
                # escape characters
                "INSERT INTO test_snapshot (a, b) VALUES (%s, %s)",
                [4, 'tes"t'],
            )
            cursor.execute(
                # the string null
                "INSERT INTO test_snapshot (a, b) VALUES (%s, %s)",
                [5, "I am NULL"],
            )
            connection.commit()

    snapshot = PostgresSnapshot(dsn)
    snapshot_id = uuid.uuid1()
    storage = FakeDestination(SnapshotId(str(snapshot_id)))
    dest = SnapshotDestination(storage)
    tables = [
        TableConfig(
            table= "test_snapshot",
            columns= ["a", "b", "c"],
        )
    ]
    desc = snapshot.dump(dest, tables)
    dest.close()
    
    assert desc.xmax == desc.xmin # There should not be any running transaciton
    assert desc.xmin is not None
    
    expected_output = (
        "META {tables} {snapshot_id}\n"
        "START {table}\n"
        "a,b,c\n"
        "1,test,2019-06-16 06:21:39+00\n"
        "2,test,\n"
        '3,"",\n'
        '4,"tes""t",\n'
        '5,'"I am NULL"',\n'
        "END TABLE\n"
        "SNAPSHOT OVER\n"
    ).format(
        tables=tables,
        snapshot_id = str(snapshot_id),
        table="test_snapshot"
    )

    assert storage.stream.getvalue() == expected_output

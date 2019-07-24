import pytest
import psycopg2

from io import StringIO

from contextlib import closing
from typing import AnyStr, IO, List
from unittest.mock import MagicMock

from cdc.snapshots.sources.postgres_snapshot import PostgresSnapshot
from cdc.snapshots.destinations import SnapshotDestination
from cdc.testutils.fixtures import dsn

class FakeDestination(SnapshotDestination):
    def __init__(self) -> None:
        super(FakeDestination, self).__init__()
        self.stream = StringIO()

    def get_name(self) -> str:
        raise NotImplementedError

    def get_stream(self) -> IO[AnyStr]:
        return self.stream

    def _set_metadata_impl(self,
        tables: List[str],
        snapshot_id: str,
    ) -> None:
        self.stream.write("META %s %s\n" % (tables, snapshot_id))

    def _start_table_impl(self, table_name:str) -> None:
        self.stream.write("START %s\n" % table_name)

    def _end_table_impl(self, table: str) -> None:
        self.stream.write("END %s\n" % table)


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
    dest = FakeDestination()
    desc = snapshot.dump(dest, ["test_snapshot"])
    
    assert desc.xmax == desc.xmin # There should not be any running transaciton
    assert desc.xmin is not None
    
    expected_output = ("META {tables} {snapshot_id}\n"
        "START {table}\n"
        "a,b,c\n"
        "1,test,2019-06-16 06:21:39+00\n"
        "2,test,\n"
        '3,"",\n'
        '4,"tes""t",\n'
        '5,'"I am NULL"',\n'
        "END {table}\n"
    ).format(
        tables=["test_snapshot"],
        snapshot_id=desc,
        table="test_snapshot"
    )

    assert dest.stream.getvalue() == expected_output

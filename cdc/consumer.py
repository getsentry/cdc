import itertools
import logging
import psycopg2
import requests
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, NamedTuple, Sequence
from urllib.parse import urljoin, urlencode

from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


class Range(NamedTuple):
    lo: Any
    hi: Any


@dataclass
class ColumnMapping(object):
    source_name: str
    destination_name: str


@dataclass
class SourceTable(object):
    name: str
    primary_key: str


@dataclass
class DestinationTable(object):
    name: str


@dataclass
class TableMapping(object):
    source: SourceTable
    destination: DestinationTable
    columns: Sequence[ColumnMapping]


class SnapshotDataExporter(ABC):
    @abstractmethod
    def dump(self, table: SourceTable, column_names: Sequence[str], range: Range):
        pass


@dataclass
class ExportTask(object):
    table: SourceTable
    range: Range


class SnapshotDataExporterManager(ABC):
    @abstractmethod
    def __enter__(self) -> SnapshotDataExporter:
        pass

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        return False

    @abstractmethod
    def get_tasks(self, tables: Iterable[SourceTable]) -> Iterator[Range]:
        pass


class PostgresSnapshotDataExporter(SnapshotDataExporter):
    def __init__(self, dsn: str, exported_snapshot: str):
        self.__dsn = dsn
        self.__exported_snapshot = exported_snapshot

    def dump(self, table: SourceTable, column_names: Sequence[str], range: Range):
        with psycopg2.connect(self.__dsn) as connection:
            connection.autocommit = False

            with connection.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY"
                )
                cursor.execute(
                    "SET TRANSACTION SNAPSHOT %s", [self.__exported_snapshot]
                )

            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="{table.name}-{range.lo}-{range.hi}".format(
                    table=table, range=range
                ),
                delete=False,  # TODO: The loader should keep track and clean this up on exit.
            ) as f, connection.cursor() as cursor:
                logger.trace("Fetching %r from %r...", range, table)
                query = "COPY (SELECT {columns} FROM {table.name} WHERE {table.primary_key} >= {range.lo} AND {table.primary_key} < {range.hi} ORDER BY {table.primary_key} ASC) TO STDOUT".format(
                    columns=", ".join(column_names), table=table, range=range
                )
                cursor.copy_expert(query, f)
                logger.trace(
                    "Fetched %r from %r (%s rows.)", range, table, cursor.rowcount
                )
                yield open(f.name, "rb")


class PostgresSnapshotDataExporterManager(SnapshotDataExporterManager):
    def __init__(self, dsn: str):
        self.__dsn = dsn
        self.__connection = None

    def __enter__(self) -> SnapshotDataExporter:
        connection = self.__connection = psycopg2.connect(self.__dsn)
        connection.autocommit = False

        with connection.cursor() as cursor:
            cursor.execute(
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE"
            )
            cursor.execute("SELECT pg_export_snapshot()")
            self.__exported_snapshot = cursor.fetchone()[0]

        return PostgresSnapshotDataExporter(self.__dsn, self.__exported_snapshot)

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.__connection.close()
        return False

    def get_tasks(self, tables: Iterable[SourceTable]) -> Iterator[Iterator[Range]]:
        for table in tables:
            chunk = 1000000
            with self.__connection.cursor() as cursor:
                cursor.execute(
                    "SELECT min({table.primary_key}), max({table.primary_key}) FROM {table.name}".format(
                        table=table
                    )
                )
                min_key, max_key = cursor.fetchone()
                logger.trace(
                    "Fetched key range for %r: min=%s, max=%s", table, min_key, max_key
                )

            def get_ranges() -> Iterator[Range]:
                for i in itertools.count(0):
                    key_lo = min_key + (chunk * i)
                    key_hi = min(min_key + (chunk * (i + 1)), max_key + 1)
                    yield Range(key_lo, key_hi)
                    if key_hi > max_key:
                        break

            yield get_ranges()


class Loader(ABC):
    @abstractmethod
    def load(self, table: DestinationTable, column_names: Sequence[str], data) -> None:
        pass


class ClickhouseLoader(Loader):
    def __init__(self, url: str, database: str):
        self.__url = url
        self.__database = database

    def load(self, table: DestinationTable, column_names: Sequence[str], data) -> None:
        for page in data:
            url = urljoin(
                self.__url,
                "?"
                + urlencode(
                    {
                        "database": self.__database,
                        "query": "INSERT INTO {table} ({columns}) FORMAT TabSeparated".format(
                            table=table.name, columns=", ".join(column_names)
                        ),
                    }
                ),
            )
            response = requests.post(url, data=page)
            response.raise_for_status()
            response.close()


def bootstrap(
    export_manager: SnapshotDataExporterManager,
    loader: Loader,
    tables: Sequence[TableMapping],
):
    with export_manager as snapshot_exporter:
        for table, ranges in zip(
            tables, export_manager.get_tasks(table.source for table in tables)
        ):
            for range in ranges:
                logger.debug("Loading data for %r (%r)...", table, range)
                loader.load(
                    table.destination,
                    [column.destination_name for column in table.columns],
                    snapshot_exporter.dump(
                        table.source,
                        [column.source_name for column in table.columns],
                        range,
                    ),
                )


if __name__ == "__main__":
    logging.basicConfig(level=5)
    exporter = PostgresSnapshotDataExporterManager(
        "postgres://postgres@localhost:5432/postgres"
    )
    loader = ClickhouseLoader("http://localhost:8123", "pgbench")
    bootstrap(
        exporter,
        loader,
        [
            TableMapping(
                SourceTable("pgbench_accounts", "aid"),
                DestinationTable("accounts"),
                [
                    ColumnMapping("aid", "account_id"),
                    ColumnMapping("bid", "branch_id"),
                    ColumnMapping("abalance", "balance"),
                ],
            ),
            TableMapping(
                SourceTable("pgbench_branches", "bid"),
                DestinationTable("branches"),
                [
                    ColumnMapping("bid", "branch_id"),
                    ColumnMapping("bbalance", "balance"),
                ],
            ),
            TableMapping(
                SourceTable("pgbench_tellers", "tid"),
                DestinationTable("tellers"),
                [
                    ColumnMapping("tid", "teller_id"),
                    ColumnMapping("bid", "branch_id"),
                    ColumnMapping("tbalance", "balance"),
                ],
            ),
        ],
    )


class Consumer(ABC):
    pass


class KafkaConsumer(Consumer):
    pass


class Writer(ABC):
    pass


class ClickhouseWriter(Writer):
    pass


def stream(consumer: Consumer, writer: Writer):
    raise NotImplementedError

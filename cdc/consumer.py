import itertools
import logging
import psycopg2
import requests
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, NamedTuple, Sequence, Union
from urllib.parse import urljoin, urlencode

from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


class Range(NamedTuple):
    lo: Any
    hi: Any


@dataclass
class SourceColumn(object):
    name: str
    format: str = '{}'


@dataclass
class DestinationColumn(object):
    name: str


@dataclass
class ColumnMapping(object):
    source: SourceColumn
    destination: DestinationColumn


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


class Exporter(ABC):
    @abstractmethod
    def dump(self, table: SourceTable, columns: Sequence[SourceColumn], range: Range):
        pass


class ExportManager(ABC):
    @abstractmethod
    def __enter__(self) -> Exporter:
        pass

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        return False

    @abstractmethod
    def get_tasks(self, tables: Iterable[SourceTable]) -> Iterator[Iterator[Range]]:
        pass


class PostgresExporter(Exporter):
    def __init__(self, dsn: str, exported_snapshot: str):
        self.__dsn = dsn
        self.__exported_snapshot = exported_snapshot

    def dump(self, table: SourceTable, columns: Sequence[SourceColumn], range: Range):
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
                    columns=", ".join([column.format.format(column.name) for column in columns]), table=table, range=range
                )
                logger.trace(query)
                cursor.copy_expert(query, f)
                logger.trace(
                    "Fetched %r from %r (%s rows, %3.2f%% full)", range, table, cursor.rowcount, (cursor.rowcount / float(range.hi - range.lo)) * 100,
                )
                return open(f.name, "rb")


class PostgresExportManager(ExportManager):
    def __init__(self, dsn: str, chunk: int = 1000000):
        self.__dsn = dsn
        self.__connection = None
        self.__chunk = chunk

    def __enter__(self) -> PostgresExporter:
        connection = self.__connection = psycopg2.connect(self.__dsn)
        connection.autocommit = False

        with connection.cursor() as cursor:
            cursor.execute(
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE"
            )
            cursor.execute("SELECT pg_export_snapshot()")
            self.__exported_snapshot = cursor.fetchone()[0]

        return PostgresExporter(self.__dsn, self.__exported_snapshot)

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.__connection.close()
        return False

    def get_tasks(self, tables: Iterable[SourceTable]) -> Iterator[Iterator[Range]]:
        for table in tables:
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
                    key_lo = min_key + (self.__chunk * i)
                    key_hi = min(min_key + (self.__chunk * (i + 1)), max_key + 1)
                    yield Range(key_lo, key_hi)
                    if key_hi > max_key:
                        break

            if min_key is None or max_key is None:
                yield iter([])
            else:
                yield get_ranges()


class Loader(ABC):
    @abstractmethod
    def load(self, table: DestinationTable, columns: Sequence[DestinationColumn], data) -> None:
        pass


class ClickhouseLoader(Loader):
    def __init__(self, url: str, database: str):
        self.__url = url
        self.__database = database

    def load(self, table: DestinationTable, columns: Sequence[DestinationTable], data) -> None:
        url = urljoin(
            self.__url,
            "?"
            + urlencode(
                {
                    "database": self.__database,
                    "query": "INSERT INTO {table} ({columns}) FORMAT TabSeparated".format(
                        table=table.name, columns=", ".join([column.name for column in columns])
                    ),
                }
            ),
        )
        response = requests.post(url, data=data)
        if response.status_code != 200:
            raise Exception(response.content)
        response.close()


from multiprocessing import Event, JoinableQueue, Process


def _setup_logging():
    logging.addLevelName(5, "TRACE")
    logging.basicConfig(
        level=5,
        format='%(asctime)s %(process)7d %(levelname)-8s %(message)s',
    )


def worker(exporter, loader, queue):
    _setup_logging()

    while True:
        table, range = queue.get()

        logger.debug("Copying data from %r to %r %r...", table.source.name, table.destination.name, range)
        try:
            loader.load(
                table.destination,
                [column.destination for column in table.columns],
                exporter.dump(
                    table.source,
                    [column.source for column in table.columns],
                    range,
                ),
            )
        except Exception as error:
            logger.error("Failed to copy data from %r to %r, %r due to error: %s", table.source.name, table.destination.name, range, error, exc_info=True)
        finally:
            queue.task_done()


def bootstrap(
    export_manager: ExportManager, loader: Loader, tables: Sequence[TableMapping], concurrency: int,
):
    with export_manager as snapshot_exporter:
        queue = JoinableQueue()
        pool = set()
        for _ in range(concurrency):
            process = Process(target=worker, args=(snapshot_exporter, loader, queue))
            process.daemon = True
            process.start()
            pool.add(process)

        for table, chunks in zip(tables, export_manager.get_tasks(table.source for table in tables)):
            for chunk in chunks:
                queue.put((table, chunk))

        queue.join()

"""

def bootstrap_pgbench():
    bootstrap(
        PostgresExportManager("postgres://postgres@localhost:5432/postgres"),
        ClickhouseLoader("http://localhost:8123", "pgbench"),
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
        concurrency=16,
    )

"""

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


if __name__ == "__main__":
    _setup_logging()

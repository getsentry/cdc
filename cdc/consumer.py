import logging
import psycopg2
import requests
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence
from urllib.parse import urljoin, urlencode

from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


@dataclass
class ColumnMapping(object):
    source_name: str
    destination_name: str


@dataclass
class TableMapping(object):
    source_name: str
    destination_name: str
    columns: Sequence[ColumnMapping]


class SnapshotDataExporter(ABC):
    @abstractmethod
    def dump(self, table_name: str, column_names: Sequence[str]):
        pass


class SnapshotDataExporterManager(ABC):
    @abstractmethod
    def __enter__(self) -> SnapshotDataExporter:
        pass

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        return False


class PostgresSnapshotDataExporter(SnapshotDataExporter):
    def __init__(self, dsn: str, exported_snapshot: str):
        self.__dsn = dsn
        self.__exported_snapshot = exported_snapshot

    def dump(self, table_name: str, column_names: Sequence[str]):
        with psycopg2.connect(self.__dsn) as connection:
            connection.autocommit = False

            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY")
                cursor.execute("SET TRANSACTION SNAPSHOT %s", [self.__exported_snapshot])

            with tempfile.NamedTemporaryFile(
                    mode="wb",
                    prefix="{}-".format(table_name),
                    delete=False,  # TODO: The loader might need to clean these up after it's done?
                ) as f, connection.cursor() as cursor:
                    cursor.copy_to(f, str(table_name), columns=column_names)
                    return open(f.name, "rb")


class PostgresSnapshotDataExporterManager(SnapshotDataExporterManager):
    def __init__(self, dsn: str):
        self.__dsn = dsn
        self.__connection = None

    def __enter__(self) -> SnapshotDataExporter:
        connection = self.__connection = psycopg2.connect(self.__dsn)
        connection.autocommit = False

        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE")
            cursor.execute("SELECT pg_export_snapshot()")
            self.__exported_snapshot = cursor.fetchone()[0]

        return PostgresSnapshotDataExporter(self.__dsn, self.__exported_snapshot)

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.__connection.close()
        return False


class Loader(ABC):
    @abstractmethod
    def load(self, table_name: str, column_names: Sequence[str], data) -> None:
        pass


class ClickhouseLoader(Loader):
    def __init__(self, url: str, database: str):
        self.__url = url
        self.__database = database

    def load(self, table_name: str, column_names: Sequence[str], data) -> None:
        url = urljoin(self.__url, "?" + urlencode({
            "database": self.__database,
            "query": "INSERT INTO {table} ({columns}) FORMAT TabSeparated".format(
                table=table_name,
                columns=", ".join(column_names),
            ),
        }))
        response = requests.post(url, data=data)
        response.raise_for_status()
        response.close()


def bootstrap(export_manager: SnapshotDataExporterManager, loader: Loader, tables: Sequence[TableMapping]):
    with export_manager as snapshot_exporter:
        for table in tables:
            logger.debug("Loading data for %r...", table)
            loader.load(
                table.destination_name,
                [column.destination_name for column in table.columns],
                snapshot_exporter.dump(table.source_name, [column.source_name for column in table.columns])
            )


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    exporter = PostgresSnapshotDataExporterManager('postgres://postgres@localhost:5432/postgres')
    loader = ClickhouseLoader('http://localhost:8123', 'pgbench')
    bootstrap(exporter, loader, [
        TableMapping('pgbench_accounts', 'accounts', [
            ColumnMapping('aid', 'account_id'),
            ColumnMapping('bid', 'branch_id'),
            ColumnMapping('abalance', 'balance'),
        ]),
    ])


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

import itertools
import json
import logging
import psycopg2
import requests
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Iterator, Mapping, NamedTuple, Sequence, Union
from urllib.parse import urljoin, urlencode
from confluent_kafka import Consumer as KafkaConsumer
from clickhouse_driver import Client
from collections import defaultdict

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
                query = "COPY (SELECT {columns}, date_trunc('seconds', now())::timestamp as mtime FROM {table.name} WHERE {table.primary_key} >= {range.lo} AND {table.primary_key} < {range.hi} ORDER BY {table.primary_key} ASC) TO STDOUT".format(
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
                    "query": "INSERT INTO {table} ({columns}, mtime) FORMAT TabSeparated".format(
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

"""


@dataclass
class Begin(object):
    timestamp: datetime


@dataclass
class Commit(object):
    pass


@dataclass
class Insert(object):
    table: str
    data: Mapping[str, Any]


@dataclass
class Update(object):
    table: str
    keys: Mapping[str, Any]
    data: Mapping[str, Any]


@dataclass
class Delete(object):
    table: str
    keys: Mapping[str, Any]


TransactionMessage = Union[Begin, Insert, Update, Delete, Commit]


class Decoder(object):
    def decode(self, payload) -> Iterator[TransactionMessage]:
        return iter([payload])


class Wal2JsonDecoder(object):
    def decode(self, payload):
        data = json.loads(payload)

        yield Begin(timestamp=datetime.strptime(data['timestamp'].split('+')[0], "%Y-%m-%d %H:%M:%S.%f"))  # XXX

        for change in data['change']:
            kind = change['kind']
            if kind == 'insert':
                yield Insert(
                    table=change['table'],
                    data=dict(zip(change['columnnames'], change['columnvalues'])),
                )
            elif kind == 'update':
                yield Update(
                    table=change['table'],
                    keys=dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues'])),
                    data=dict(zip(change['columnnames'], change['columnvalues'])),
                )
            elif kind == 'delete':
                yield Delete(
                    table=change['table'],
                    keys=dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues'])),
                )
            else:
                raise NotImplementedError

        yield Commit()


class KafkaConsumerBackend(object):
    def __init__(self, topic, options):
        self.__topic = topic
        self.__consumer = KafkaConsumer(options)
        self.__consumer.subscribe([topic])

    def poll(self):
        message = self.__consumer.poll()
        if message is not None:
            return message.value()


class Consumer(object):
    def __init__(self, backend, decoder):
        self.__backend = backend
        self.__decoder = decoder

    def poll(self):
        message = self.__backend.poll()
        if message is not None:
            return self.__decoder.decode(message)


class StreamWriter(object):
    def write(self, messages: Iterator[TransactionMessage]):
        for message in messages:
            print(message)


class ClickhouseWriter(object):
    def __init__(self, client):
        self.__client = client

    def write(self, messages: Iterator[TransactionMessage]):
        try:
            begin = next(messages)
        except StopIteration:
            return

        mtime = begin.timestamp
        logger.trace('Starting transaction: %r', begin)

        # collect records by type and table
        inserts = defaultdict(list)
        deletes = defaultdict(list)

        for m in messages:
            if isinstance(m, Insert):
                inserts[m.table].append(m)
            elif isinstance(m, Update):
                # all updates create a replacing insert
                inserts[m.table].append(m)
                # if the primary key changed, this is also a deletion on the
                # old record since it will not be replaced
                if not all(m.data[k] == v for k, v in m.keys.items()):
                    deletes[m.table].append(m)
            elif isinstance(m, Delete):
                deletes[m.table].append(m)

        for t, ms in inserts.items():
            columns = set(ms[0].data)  # XXX
            query = 'INSERT INTO {table} ({columns}, mtime) VALUES'.format(table=t, columns=', '.join(columns))
            params = [{'mtime': mtime, **m.data} for m in ms]
            self.__client.execute(query, params)

        for t, ms in deletes.items():
            print('would write', len(ms), 'deletes to', t)

        logger.trace('Completed transaction')


def stream(consumer: Consumer, writer: StreamWriter, tables):
    rewrite_changes = mapper(tables)

    while True:
        messages = consumer.poll()
        if messages is None:
            continue

        writer.write(rewrite_changes(messages))


def mapper(tables):
    tm = {t.source.name: (t.destination.name, {c.source.name: c.destination.name for c in t.columns}) for t in tables}

    def apply(messages):
        has_change = False
        begin = next(messages)
        for m in messages:
            if isinstance(m, (Insert, Update, Delete)):
                if m.table not in tm:
                    print('skipping ', m.table)
                    continue
                if not has_change:
                    yield begin
                    has_change = True

                table, columns = tm[m.table]
                if isinstance(m, Insert):
                    yield Insert(table=table, data={columns[k]: v for k, v in m.data.items() if k in columns})
                elif isinstance(m, Update):
                    yield Update(table=table, keys={columns[k]: v for k, v in m.keys.items() if k in columns}, data={columns[k]: v for k, v in m.data.items() if k in columns})
                elif isinstance(m, Delete):
                    yield Delete(table=table, keys={columns[k]: v for k, v in m.keys.items() if k in columns})
                else:
                    raise Exception
            elif isinstance(m, Commit):
                if has_change:
                    yield m

    return apply



pgbench_tables = [
    TableMapping(
        SourceTable("pgbench_accounts", "aid"),
        DestinationTable("accounts"),
        [
            ColumnMapping(SourceColumn("aid"), DestinationColumn("account_id")),
            ColumnMapping(SourceColumn("bid"), DestinationColumn("branch_id")),
            ColumnMapping(SourceColumn("abalance"), DestinationColumn("balance")),
        ],
    ),
    TableMapping(
        SourceTable("pgbench_branches", "bid"),
        DestinationTable("branches"),
        [
            ColumnMapping(SourceColumn("bid"), DestinationColumn("branch_id")),
            ColumnMapping(SourceColumn("bbalance"), DestinationColumn("balance")),
        ],
    ),
    TableMapping(
        SourceTable("pgbench_tellers", "tid"),
        DestinationTable("tellers"),
        [
            ColumnMapping(SourceColumn("tid"), DestinationColumn("teller_id")),
            ColumnMapping(SourceColumn("bid"), DestinationColumn("branch_id")),
            ColumnMapping(SourceColumn("tbalance"), DestinationColumn("balance")),
        ],
    ),
]


def test_bootstrap():
    return bootstrap(
        PostgresExportManager("postgres://postgres@localhost:5432/pgbench"),
        ClickhouseLoader("http://localhost:8123", "pgbench"),
        pgbench_tables,
        concurrency=16,
    )


def test_stream():
    import uuid
    consumer = Consumer(
        KafkaConsumerBackend('topic', {
            'bootstrap.servers': 'localhost:9092',
            'group.id': uuid.uuid1().hex,
            'enable.partition.eof': 'false',
        }),
        Wal2JsonDecoder(),
    )
    writer = ClickhouseWriter(
        Client('localhost', database='pgbench'),
    )
    return stream(consumer, writer, pgbench_tables)


if __name__ == "__main__":
    _setup_logging()

import itertools
import json
import logging
import psycopg2
import requests
import tempfile
from clickhouse_driver import Client
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures.process import ProcessPoolExecutor
from confluent_kafka import Consumer as KakfaConsumer
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from psycopg2 import sql
from typing import Any, BinaryIO, Iterable, Iterator, Generator, MutableMapping, MutableSequence, NewType, Sequence, Tuple, Union
from urllib.parse import urljoin, urlencode

from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


@dataclass(frozen=True)
class Column:
    name: str


@dataclass(frozen=True)
class Table:
    name: str
    identity_columns: Sequence[Column]
    data_columns: Sequence[Column]

    def __post_init__(self) -> None:
        assert len(self.identity_columns) > 0

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def columns(self) -> Iterator[Column]:
        return itertools.chain(self.identity_columns, self.data_columns)


@dataclass(frozen=True)
class Source:
    table: Table


@dataclass(frozen=True)
class Destination:
    table: Table
    version_column: Column


@dataclass(frozen=True)
class TableMapping:  # not the best name
    source: Source
    destination: Destination

    def __post_init__(self) -> None:
        assert len(self.source.table.identity_columns) == len(self.destination.table.identity_columns)
        assert len(self.source.table.data_columns) == len(self.destination.table.data_columns)


dump_dsn = 'postgres://postgres@localhost:5432/pgbench'

SnapshotIdentifier = NewType('SnapshotIdentifier', str)


@dataclass(frozen=True)
class Snapshot:
    xmin: int
    xmax: int
    xip_list: Sequence[int]


@contextmanager
def export_snapshot() -> Generator[Tuple[SnapshotIdentifier, Snapshot], None, None]:
    connection = psycopg2.connect(dump_dsn)
    connection.autocommit = False

    with connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE")
        cursor.execute("SELECT txid_current_snapshot(), pg_export_snapshot()")
        current_snapshot, snapshot_identifier = cursor.fetchone()
        xmin, xmax, xip_list = current_snapshot.split(':')
        yield (
            SnapshotIdentifier(snapshot_identifier),
            Snapshot(int(xmin), int(xmax), [int(xip) for xip in (xip_list or [])]),
        )

    connection.close()


def dump(snapshot_identifier: SnapshotIdentifier, source: Source) -> BinaryIO:
    logger.debug('Dumping data from %r using snapshot: %s...', source, snapshot_identifier)

    connection = psycopg2.connect(dump_dsn)
    with connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY")
        cursor.execute("SET TRANSACTION SNAPSHOT %s", [snapshot_identifier])

    output = tempfile.NamedTemporaryFile(
        mode="wb",
        prefix="{table.name}".format(table=source.table),
        delete=False,  # TODO: The loader should keep track and clean this up on exit.
    )

    with connection.cursor() as cursor, output as f:
        cursor.copy_expert(
            sql.SQL("COPY (SELECT {columns}, date_trunc('seconds', now())::timestamp FROM {table}) TO STDOUT").format(
                columns=sql.SQL(', ').join(map(sql.Identifier, (column.name for column in source.table.columns))),
                table=sql.Identifier(source.table.name),
            ),
            f,
        )
        logger.debug('Dumped %s rows from %r.', cursor.rowcount, source)

    return open(output.name, 'rb')


load_url = 'http://localhost:8123'
load_database = 'pgbench'

def load(destination: Destination, data: BinaryIO) -> None:
    logger.debug('Loading data into %r...', destination)
    response = requests.post(
        urljoin(load_url, '?' + urlencode({
            'database': load_database,
            'query': "INSERT INTO {table} ({columns}) FORMAT TabSeparated".format(
                columns=', '.join(column.name for column in itertools.chain(destination.table.columns, [destination.version_column])),
                table=destination.table.name,
            ),
        })),
        data=data,
    )
    if response.status_code != 200:
        raise Exception(response.content)
    response.close()


def copy(snapshot_identifier: SnapshotIdentifier, table_mapping: TableMapping) -> None:
    load(table_mapping.destination, dump(snapshot_identifier, table_mapping.source))


def bootstrap(table_mappings: Iterable[TableMapping]) -> Snapshot:
    with ProcessPoolExecutor() as pool, export_snapshot() as (snapshot_identifier, snapshot):
        futures = [pool.submit(copy, snapshot_identifier, table_mapping) for table_mapping in table_mappings]
        for future in as_completed(futures):
            future.result()
    return snapshot


@dataclass(frozen=True)
class Transaction:
    xid: int
    timestamp: datetime


@dataclass(frozen=True)
class Begin:
    transaction: Transaction


@dataclass(frozen=True)
class Insert:
    table: Table
    new_identity_values: Sequence[Any]
    new_data_values: Sequence[Any]


@dataclass(frozen=True)
class Update:
    table: Table
    old_identity_values: Sequence[Any]
    new_identity_values: Sequence[Any]
    new_data_values: Sequence[Any]


@dataclass(frozen=True)
class Delete:
    table: Table
    old_identity_values: Sequence[Any]


@dataclass(frozen=True)
class Commit:
    pass


Mutation = Union[Insert, Update, Delete]
Change = Union[Begin, Mutation, Commit]


def decode(table_mappings: Iterable[TableMapping], message: str) -> Iterator[Change]:
    tables = {table_mapping.source.table.name: table_mapping for table_mapping in table_mappings}

    data = json.loads(message)

    yield Begin(
        Transaction(
            timestamp=datetime.strptime(data["timestamp"].split("+")[0], "%Y-%m-%d %H:%M:%S.%f"),  # XXX
            xid=int(data['xid']),
        ),
    )

    for change in data["change"]:
        kind = change["kind"]
        table = change["table"]
        table_mapping = tables.get(table)
        if table_mapping is None:
            logger.trace('Discarded change from unmapped table: %r', table)
            continue

        source_table = table_mapping.source.table
        destination_table = table_mapping.destination.table
        if kind == "insert":
            yield Insert(
                table=destination_table,
                new_identity_values=[change["columnvalues"][i] for i in (change["columnnames"].index(column.name) for column in source_table.identity_columns)],
                new_data_values=[change["columnvalues"][i] for i in (change["columnnames"].index(column.name) for column in source_table.data_columns)],
            )
        elif kind == "update":
            yield Update(
                table=destination_table,
                old_identity_values=[change["oldkeys"]["keyvalues"][i] for i in (change["oldkeys"]["keynames"].index(column.name) for column in source_table.identity_columns)],
                new_identity_values=[change["columnvalues"][i] for i in (change["columnnames"].index(column.name) for column in source_table.identity_columns)],
                new_data_values=[change["columnvalues"][i] for i in (change["columnnames"].index(column.name) for column in source_table.data_columns)],
            )
        elif kind == "delete":
            yield Delete(
                table=destination_table,
                old_identity_values=[change["oldkeys"]["keyvalues"][i] for i in (change["oldkeys"]["keynames"].index(column.name) for column in source_table.identity_columns)],
            )
        else:
            raise Exception

    yield Commit()


import uuid

stream_consumer_topic = 'topic'
stream_consumer_options = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': uuid.uuid1().hex,
    'auto.offset.reset': 'latest',
    'enable.partition.eof': 'false',
}

def consumer(table_mappings: Iterable[TableMapping]) -> Iterator[Change]:
    logger.debug('Starting to consume from stream...')

    consumer = KakfaConsumer(stream_consumer_options)
    consumer.subscribe([stream_consumer_topic])

    while True:
        message = consumer.poll()
        if message is None:
            continue

        for change in decode(table_mappings, message.value()):
            yield change


writer_host = "localhost"
writer_database = "pgbench"

def writer(changes: Iterator[Change], batch: int=1000) -> None:
    client = Client(writer_host, database=writer_database)

    inserts: MutableMapping[Table, MutableSequence[Sequence[Any]]] = defaultdict(list)
    deletes: MutableMapping[Table, MutableSequence[Sequence[Any]]] = defaultdict(list)

    pending = 0

    for change in changes:
        if not isinstance(change, (Insert, Update, Delete)):
            logger.trace('Discarding non-mutation message: %r', change)
            continue

        if isinstance(change, Insert):
            inserts[change.table].append([*change.new_identity_values, *change.new_data_values])
        elif isinstance(change, Update):
            inserts[change.table].append([*change.new_identity_values, *change.new_data_values])
            if change.old_identity_values != change.new_identity_values:
                deletes[change.table].append(change.old_identity_values)
        elif isinstance(change, Delete):
            deletes[change.table].append(change.old_identity_values)
        else:
            raise Exception

        pending += 1

        if pending > batch:
            for table in list(inserts.keys()):
                client.execute(
                    "INSERT INTO {table.name} ({columns}) VALUES".format(
                        table=table,
                        columns=', '.join(column.name for column in table.columns),
                    ),
                    inserts.pop(table),
                )

            for table in list(deletes.keys()):
                # TODO
                deletes.pop(table)


def stream(table_mappings: Iterable[TableMapping], snapshot: Snapshot) -> None:
    # TODO: Snapshot filtering
    writer(consumer(table_mappings))


def setup_logging() -> None:
    logging.addLevelName(5, "TRACE")
    logging.basicConfig(
        level=5, format="%(asctime)s %(process)7d %(levelname)-8s %(message)s"
    )


def test() -> None:
    setup_logging()

    table_mappings = [
        TableMapping(
            Source(
                Table('pgbench_branches', [Column('bid')], [Column('bbalance')]),
            ),
            Destination(
                Table('branches', [Column('branch_id')], [Column('balance')]),
                Column('mtime'),
            ),
        ),
        TableMapping(
            Source(
                Table('pgbench_accounts', [Column('aid')], [Column('bid'), Column('abalance')]),
            ),
            Destination(
                Table('accounts', [Column('account_id')], [Column('branch_id'), Column('balance')]),
                Column('mtime'),
            ),
        ),
        TableMapping(
            Source(
                Table('pgbench_tellers', [Column('tid')], [Column('bid'), Column('tbalance')]),
            ),
            Destination(
                Table('tellers', [Column('teller_id')], [Column('branch_id'), Column('balance')]),
                Column('mtime'),
            ),
        ),
    ]

    snapshot = bootstrap(table_mappings)

    stream(table_mappings, snapshot)

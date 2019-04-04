import itertools
import logging
import psycopg2
import requests
import tempfile
from concurrent.futures import ALL_COMPLETED, wait
from concurrent.futures.process import ProcessPoolExecutor
from dataclasses import dataclass
from psycopg2 import sql
from typing import BinaryIO, Iterable, Iterator, Sequence
from urllib.parse import urljoin, urlencode

from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


@dataclass
class Column:
    name: str


@dataclass
class Table:
    name: str
    identity_columns: Sequence[Column]
    data_columns: Sequence[Column]

    def __post_init__(self) -> None:
        assert len(self.identity_columns) > 0

    @property
    def columns(self) -> Iterator[Column]:
        return itertools.chain(self.identity_columns, self.data_columns)


@dataclass
class Source:
    table: Table


@dataclass
class Destination:
    table: Table
    version_column: Column


@dataclass
class TableMapping:  # not the best name
    source: Source
    destination: Destination

    def __post_init__(self) -> None:
        assert len(self.source.table.identity_columns) == len(self.destination.table.identity_columns)
        assert len(self.source.table.data_columns) == len(self.destination.table.data_columns)


dump_dsn = 'postgres://postgres@localhost:5432/pgbench'

def dump(source: Source) -> BinaryIO:
    logger.debug('Dumping data from %r...', source)

    connection = psycopg2.connect(dump_dsn)

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


def copy(table_mapping: TableMapping) -> None:
    load(table_mapping.destination, dump(table_mapping.source))


def bootstrap(table_mappings: Iterable[TableMapping]) -> None:
    with ProcessPoolExecutor() as pool:
        futures = [pool.submit(copy, table_mapping) for table_mapping in table_mappings]
        # TODO: Do something useful with this result.
        wait(futures, return_when=ALL_COMPLETED)


def setup_logging() -> None:
    logging.addLevelName(5, "TRACE")
    logging.basicConfig(
        level=5, format="%(asctime)s %(process)7d %(levelname)-8s %(message)s"
    )


def test_bootstrap() -> None:
    setup_logging()
    bootstrap([
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
                Table('pgbench_accounts', [Column('bid'), Column('aid')], [Column('abalance')]),
            ),
            Destination(
                Table('accounts', [Column('branch_id'), Column('account_id')], [Column('balance')]),
                Column('mtime'),
            ),
        ),
        TableMapping(
            Source(
                Table('pgbench_tellers', [Column('bid'), Column('tid')], [Column('tbalance')]),
            ),
            Destination(
                Table('tellers', [Column('branch_id'), Column('teller_id')], [Column('balance')]),
                Column('mtime'),
            ),
        ),
    ])

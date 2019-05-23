import functools
import os
import psycopg2
import pytest
import time
import uuid
from contextlib import closing
from psycopg2.sql import SQL, Identifier

from cdc.sources.backends.postgres_logical import PostgresLogicalReplicationSlotBackend


def wait(callable, test, attempts=10, delay=1):
    for attempt in range(1, attempts + 1):
        value = callable()
        if test(value):
            return value
        elif attempt != attempts:
            time.sleep(delay)

    raise AssertionError(
        f"{callable} did not return a value that passes {test} within {attempts * delay} seconds"
    )


@pytest.fixture
def dsn():
    template = os.environ.get("CDC_POSTGRES_DSN_TEMPLATE", "postgres:///{database}")
    database_name = "cdc_test_{uuid}".format(uuid=uuid.uuid1().hex)

    with closing(psycopg2.connect(template.format(database="postgres"))) as connection:
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(SQL("CREATE DATABASE {}").format(Identifier(database_name)))

        try:
            yield template.format(database=database_name)
        finally:
            with connection.cursor() as cursor:
                cursor.execute(
                    SQL("DROP DATABASE {}").format(Identifier(database_name))
                )


@pytest.fixture
def slot_name(dsn):
    slot_name = "cdc_test_{uuid}".format(uuid=uuid.uuid1().hex)

    with closing(psycopg2.connect(dsn)) as connection:
        try:
            yield slot_name
        finally:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT active_pid, pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = %s AND active_pid IS NOT NULL",
                    [slot_name],
                )
                wait(
                    lambda: cursor.execute(
                        "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                        [slot_name],
                    ),
                    lambda active: not active,
                    attempts=30,
                    delay=0.1,
                )
                cursor.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                    [slot_name],
                )


def test(dsn, slot_name):
    create_backend = functools.partial(
        PostgresLogicalReplicationSlotBackend,
        dsn=dsn,
        slot_name=slot_name,
        slot_plugin="test_decoding",
    )

    with pytest.raises(psycopg2.ProgrammingError):
        create_backend(slot_create=False).fetch()

    with closing(psycopg2.connect(dsn)) as connection:
        connection.autocommit = False

        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE select_table_1 (a integer, b text, primary key(a))"
            )
            connection.commit()

        backend = create_backend(slot_create=True)
        assert backend.fetch() is None

        with connection.cursor() as cursor:
            cursor.execute("SELECT txid_current()")
            xid = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO select_table_1 (a, b) VALUES (%s, %s)", [1, "test"]
            )
            connection.commit()

        result = wait(backend.fetch, lambda result: result is not None)
        assert result[1] == f"BEGIN {xid}".encode("ascii")

        result = wait(backend.fetch, lambda result: result is not None)
        assert (
            result[1]
            == b"table public.select_table_1: INSERT: a[integer]:1 b[text]:'test'"
        )

        result = wait(backend.fetch, lambda result: result is not None)
        assert result[1] == f"COMMIT {xid}".encode("ascii")
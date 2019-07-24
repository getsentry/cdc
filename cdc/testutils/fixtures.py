import os
import pytest
import psycopg2
import uuid

from contextlib import closing
from psycopg2.sql import SQL, Identifier

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
                # There could be lingering connections to the database. This can
                # happen if we are able to establish a connection to the database
                # but cannot create a replication slot.
                #
                # Given that this is a test and we want to delete the test database,
                # we may first have to kill all connections to the test database.
                #
                # The following line will not delete our current connection cause
                # we are connected to the "postgres" database.
                cursor.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s",
                    [database_name]
                )

                cursor.execute(
                    SQL("DROP DATABASE {}").format(Identifier(database_name))
                )

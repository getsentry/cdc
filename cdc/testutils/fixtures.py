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
                cursor.execute(
                    SQL("DROP DATABASE {}").format(Identifier(database_name))
                )

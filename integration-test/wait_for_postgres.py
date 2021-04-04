import os
import time
from contextlib import closing

import psycopg2
from psycopg2.sql import SQL, Identifier

template = os.environ.get("CDC_POSTGRES_DSN_TEMPLATE", "postgres:///{database}")

for i in range(1, 11):
    try:
        print(
            f"Attempting to connect to postgres {template.format(database='postgres')}. Attempt {i} of 10"
        )
        with closing(
            psycopg2.connect(template.format(database="postgres"))
        ) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(SQL("CREATE DATABASE {}").format(Identifier("test_db")))

        exit(0)
    except Exception as e:
        print(f"Could not connect. Waiting 1 second")
        time.sleep(1)

exit(-1)

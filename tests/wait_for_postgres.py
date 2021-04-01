import psycopg2
import os
import time

template = os.environ.get("CDC_POSTGRES_DSN_TEMPLATE", "postgres:///{database}")

for i in range(1, 6):
    try:
        print(
            f"Attempting to connect to postgres {template.format(database='postgres')}. Attempt {i} of 5"
        )
        psycopg2.connect(template.format(database="postgres"))
        exit(0)
    except Exception as e:
        print(e)
        print(f"Could not connect. Waiting 1 second")
        time.sleep(1)

exit(-1)

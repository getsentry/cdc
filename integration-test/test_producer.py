import os
import re
import time
from contextlib import closing

import psycopg2
from confluent_kafka import Consumer

DSN_TEMPLATE = os.environ.get("CDC_POSTGRES_DSN_TEMPLATE", "postgres:///{database}")
DATABASE_NAME = "test_db"


def _wait_for_slot() -> None:
    with closing(
        psycopg2.connect(DSN_TEMPLATE.format(database="postgres"))
    ) as connection:
        for i in range(1, 10):
            print("Waiting for slot")
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM pg_replication_slots;")
                for row in cursor:
                    print("Found slot")
                    return
            time.sleep(1)


def test_producer() -> None:
    _wait_for_slot()
    with closing(
        psycopg2.connect(DSN_TEMPLATE.format(database="test_db"))
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE test_producer (a integer, b text, c timestamptz, primary key(a))"
            )
            cursor.execute(
                "INSERT INTO test_producer (a, b, c) VALUES (%s, %s, %s)",
                [1, "test", "2019-06-16 06:21:39+00"],
            )
            connection.commit()

    conf = {
        "bootstrap.servers": "kafka:9092",
        "auto.offset.reset": "smallest",
        "group.id": "test",
    }

    consumer = Consumer(conf)
    consumer.subscribe(["cdc"])

    def assert_message(regex: str) -> None:
        message = consumer.poll(timeout=60)
        assert message is not None
        value = message.value().decode("utf-8")
        result = re.search(regex, value)
        assert (
            result is not None
        ), f"Unexpected message: {value}. Expected regex {regex}"

    assert_message(r"BEGIN \d+")
    assert_message(
        r"INSERT\: a\[integer\]\:1 b\[text\]\:'test' c\[timestamp with time zone\]\:'2019\-06\-16 06\:21\:39\+00'"
    )
    assert_message(r"COMMIT .+")

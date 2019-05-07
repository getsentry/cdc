cdc
###

Quick Start Example
===================

1. Start services in ``sentry`` via ``sentry devservices up``.
2. Create a ``pgbench`` database::

    docker exec sentry_postgres createdb -U postgres pgbench

3. Start a producer::

    docker run --net sentry --rm $(docker build -q .) producer

4. Initialize the ``pgbench`` tables and run a quick benchmark::

    docker exec sentry_postgres pgbench -U postgres -i pgbench
    docker exec sentry_postgres pgbench -U postgres -T 10 pgbench

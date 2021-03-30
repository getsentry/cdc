#!/bin/bash
set -e

{ echo "host replication $POSTGRES_USER 0.0.0.0/0 trust"; } >> "$PGDATA/pg_hba.conf"
{ echo "host replication $POSTGRES_USER ::1/128 trust"; } >> "$PGDATA/pg_hba.conf"

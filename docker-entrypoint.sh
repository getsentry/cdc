#!/bin/bash

# first check if we're passing flags, if so
# prepend with cdc
if [ "${1:0:1}" = '-' ]; then
    set -- cdc "$@"
fi

# Check for a valid cdc subcommand
if [ "${1:0:1}" != '/' ] && cdc "$1" --help > /dev/null 2>&1; then
    set -- cdc "$@"
fi

if [ "$1" = 'cdc' -a "$(id -u)" = '0' ]; then
    set -- gosu cdc tini -- "$@"
fi

exec "$@"

import itertools
import logging
from datetime import datetime, timedelta
from select import select
from typing import Set, Union, Tuple

from cdc.common import Id, Message, Position, ScheduledTask


logger = logging.getLogger(__name__)


backends = {}


class Source(object):
    schema = {
        'type': 'object',
        'properties': {
            'backend': {
                'type': 'object',
                'properties': {
                    'type': {'type': 'string'},
                    'options': {'type': 'object'},
                },
                'required': ['type'],
            },
            'commit_positions_after_flushed_messages': {'type': 'number'},
            'commit_positions_after_seconds': {'type': 'number'},
        },
        'required': ['backend'],
    }

    def __init__(self, configuration):
        self.__backend = backends[configuration['backend']['type']](configuration['backend']['options'])

        self.__id_generator = itertools.count(1)

        self.__commit_messages = configuration['commit_positions_after_flushed_messages']
        self.__commit_timeout = configuration['commit_positions_after_seconds']

        self.__write_id: Union[None, Id] = None
        self.__write_position: Union[None, Position] = None

        self.__flush_id: Union[None, Id] = None
        self.__flush_position: Union[None, Position] = None

        self.__last_commit_flush_id: Union[None, Id] = None
        self.__last_commit_datetime: datetime = datetime.now()  # TODO: This is kind of a strange default

    def __repr__(self):
        return '<{type}: {backend}>'.format(
            type=type(self).__name__,
            backend=self.__backend,
        )

    def fetch(self) -> Union[None, Message]:
        result = self.__backend.fetch()
        if result is None:
            return

        return Message(
            Id(next(self.__id_generator)),
            result[0],
            result[1],
        )

    def poll(self, timeout: float = None):
        self.__backend.poll(timeout)

    def set_write_position(self, id: Id, position: Position):
        logger.trace('Updating write position of %r to %s...', self, position)
        assert (self.__write_id or 0) + 1 == id
        self.__write_id = id
        self.__write_position = position

    def set_flush_position(self, id: Id, position: Position):
        logger.trace('Updating flush position of %r to %s...', self, position)
        assert (self.__flush_id or 0) + 1 == id
        self.__flush_id = id
        self.__flush_position = position

    def commit_positions(self):
        logger.trace('Committing positions: write=%r, flush=%r', self.__write_position, self.__flush_position)
        self.__backend.commit_positions(self.__write_position, self.__flush_position)
        self.__last_commit_flush_id = self.__flush_id
        self.__last_commit_datetime = datetime.now()

    def get_next_scheduled_task(self, now: datetime) -> ScheduledTask:
        if self.__flush_id is not None and self.__flush_id - (self.__last_commit_flush_id if self.__last_commit_flush_id is not None else 0) > self.__commit_messages:
            return ScheduledTask(now, self.commit_positions)

        task = ScheduledTask(
            self.__last_commit_datetime + timedelta(seconds=self.__commit_timeout),
            self.commit_positions,
        )

        backend_task = self.__backend.get_next_scheduled_task(now)
        if backend_task is not None and task > backend_task:
            task = backend_task

        return task


class SourceBackend(object):
    def fetch(self) -> Union[None, Tuple[Position, str]]:
        raise NotImplementedError

    def poll(self, timeout: float):
        raise NotImplementedError

    def commit_positions(self, write_position: Union[None, Position], flush_position: Union[None, Position]):
        raise NotImplementedError

    def get_next_scheduled_task(self, now: datetime) -> Union[None, ScheduledTask]:
        raise NotImplementedError


import psycopg2
from psycopg2.extras import (
    LogicalReplicationConnection,
    REPLICATION_LOGICAL,
)


class PostgresLogicalReplicationSlotBackend(SourceBackend):
    schema = {
        'type': 'object',
        'properties': {
            'dsn': {'type': 'string'},
            'slot': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'plugin': {'type': 'string'},
                    'create': {'type': 'boolean'},
                    'options': {
                        'type': 'object',
                        'properties': {},  # TODO
                    }
                },
                'required': ['name', 'plugin'],
            },
            'keepalive_interval': {'type': 'number'},
        },
        'required': ['dsn'],
    }

    def __init__(self, configuration):
        self.__dsn = configuration['dsn']
        self.__slot_name = configuration['slot']['name']
        self.__slot_plugin = configuration['slot']['plugin']
        self.__slot_options = configuration['slot']['options']
        self.__slot_create = configuration['slot']['create']
        self.__keepalive_interval = configuration['keepalive_interval']
        self.__cursor = None  # TODO: type

    def __repr__(self):
        return '<{type}: {slot!r} on {dsn!r}>'.format(
            type=type(self).__name__,
            slot=self.__slot_name,
            dsn=self.__dsn,
        )

    def __get_cursor(self, create: bool = False):
        if self.__cursor is not None:
            return self.__cursor
        elif not create:
            raise Exception('cursor not already established')

        logger.debug('Establishing replication connection to %r...', self.__dsn)
        self.__cursor = psycopg2.connect(
            self.__dsn,
            connection_factory=LogicalReplicationConnection,
        ).cursor()

        if self.__slot_create:
            logger.debug("Creating replication slot %r using %r, if it doesn't already exist...", self.__slot_name, self.__slot_plugin)
            try:
                self.__cursor.create_replication_slot(
                    self.__slot_name,
                    REPLICATION_LOGICAL,
                    self.__slot_plugin,
                )
            except psycopg2.ProgrammingError:
                logger.debug('Replication slot already exists.')
            else:
                logger.debug('Replication slot created.')

        logger.debug('Starting replication on %r...', self.__cursor)
        self.__cursor.start_replication(
            self.__slot_name,
            REPLICATION_LOGICAL,
            options=self.__slot_options,
        )

        return self.__cursor

    def fetch(self) -> Union[None, Tuple[Position, str]]:
        message = self.__get_cursor(create=True).read_message()
        if message is None:
            return

        return (
            Position(message.data_start),
            message.payload,
        )

    def poll(self, timeout: float):
        select([
            self.__get_cursor(),
        ], [], [], timeout)

    def commit_positions(self, write_position: Union[None, Position], flush_position: Union[None, Position]):
        send_feedback_kwargs = {}

        if write_position is not None:
            send_feedback_kwargs['write_lsn'] = write_position

        if flush_position is not None:
            send_feedback_kwargs['flush_lsn'] = flush_position

        self.__get_cursor().send_feedback(**send_feedback_kwargs)

    def send_keepalive(self):
        """
        Send a keep-alive message.
        """
        self.__get_cursor().send_feedback()

    def get_next_scheduled_task(self, now: datetime) -> Union[None, ScheduledTask]:
        return ScheduledTask(
            self.__get_cursor(create=False).io_timestamp + timedelta(seconds=self.__keepalive_interval),
            self.send_keepalive,
        )


backends['postgres:logical'] = PostgresLogicalReplicationSlotBackend

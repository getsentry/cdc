import itertools
import logging
from datetime import datetime, timedelta
from typing import Union

from cdc.common import Id, Message, Position, ScheduledTask
from cdc.logging import LoggerAdapter
from cdc.sources.backends import SourceBackend


logger = LoggerAdapter(logging.getLogger(__name__))


class Source(object):
    schema = {
        "type": "object",
        "properties": {
            "backend": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "options": {"type": "object"},
                },
                "required": ["type"],
            },
            "commit_positions_after_flushed_messages": {"type": "number"},
            "commit_positions_after_seconds": {"type": "number"},
        },
        "required": ["backend"],
    }

    def __init__(self, configuration):
        self.__backend: SourceBackend = backends[configuration["backend"]["type"]](
            configuration["backend"]["options"]
        )

        self.__id_generator = itertools.count(1)

        self.__commit_messages = configuration[
            "commit_positions_after_flushed_messages"
        ]
        self.__commit_timeout = configuration["commit_positions_after_seconds"]

        self.__write_id: Union[None, Id] = None
        self.__write_position: Union[None, Position] = None

        self.__flush_id: Union[None, Id] = None
        self.__flush_position: Union[None, Position] = None

        self.__last_commit_flush_id: Union[None, Id] = None
        self.__last_commit_datetime: datetime = datetime.now()  # TODO: This is kind of a strange default

    def __repr__(self):
        return "<{type}: {backend}>".format(
            type=type(self).__name__, backend=self.__backend
        )

    def validate(self):
        self.__backend.validate()

    def fetch(self) -> Union[None, Message]:
        result = self.__backend.fetch()
        if result is not None:
            return Message(Id(next(self.__id_generator)), result[0], result[1])
        else:
            return None

    def poll(self, timeout: float):
        self.__backend.poll(timeout)

    def set_write_position(self, id: Id, position: Position):
        logger.trace("Updating write position of %r to %s...", self, position)
        assert (self.__write_id or 0) + 1 == id
        self.__write_id = id
        self.__write_position = position

    def set_flush_position(self, id: Id, position: Position):
        logger.trace("Updating flush position of %r to %s...", self, position)
        assert (self.__flush_id or 0) + 1 == id
        self.__flush_id = id
        self.__flush_position = position

    def commit_positions(self):
        logger.trace("Committing positions...")
        self.__backend.commit_positions(self.__write_position, self.__flush_position)
        logger.debug(
            "Updated committed positions: write=%r, flush=%r",
            self.__write_position,
            self.__flush_position,
        )
        self.__last_commit_flush_id = self.__flush_id
        self.__last_commit_datetime = datetime.now()

    def get_next_scheduled_task(self, now: datetime) -> ScheduledTask:
        if (
            self.__flush_id is not None
            and self.__flush_id
            - (
                self.__last_commit_flush_id
                if self.__last_commit_flush_id is not None
                else 0
            )
            > self.__commit_messages
        ):
            return ScheduledTask(now, self.commit_positions)

        task = ScheduledTask(
            self.__last_commit_datetime + timedelta(seconds=self.__commit_timeout),
            self.commit_positions,
        )

        backend_task = self.__backend.get_next_scheduled_task(now)
        if backend_task is not None and task > backend_task:
            task = backend_task

        return task

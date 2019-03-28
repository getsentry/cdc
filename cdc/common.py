from datetime import datetime
from typing import NamedTuple, NewType, Callable


Id = NewType('Id', int)
Position = NewType('Position', int)


class ScheduledTask(NamedTuple):
    deadline: datetime
    callable: Callable[[], None]


class Message(NamedTuple):
    id: Id
    position: Position
    payload: bytes

from typing import NamedTuple, NewType


Id = NewType("Id", int)
Position = NewType("Position", int)


class Message(NamedTuple):
    id: Id
    position: Position
    payload: bytes

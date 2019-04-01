from typing import NamedTuple, NewType


Id = NewType("Id", int)
Position = NewType("Position", int)
Payload = NewType("Payload", bytes)


class Message(NamedTuple):
    id: Id
    position: Position
    payload: Payload

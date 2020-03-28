from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import NamedTuple, NewType


Id = NewType("Id", int)
Position = NewType("Position", int)
Payload = NewType("Payload", bytes)


class Message(NamedTuple):
    """
    Represents a replication message.
    """

    # A sequential identifier which can be used to ensure that messages are
    # being delivered in the correct order. This sequence is initialized to 1
    # at process startup, and doesn't have any particular significance outside
    # of the running process.
    id: Id

    payload: MsgPayload


@dataclass(frozen=True)
class MsgPayload(ABC):
    # The current replication position. This value is provided by the source,
    # and the meaning of the value is dependent on which source is being used.
    position: Position

    # The replication data payload. This value is provided by the source, and
    # the meaning of the value is dependent on which source is being used.
    payload: Payload


@dataclass(frozen=True)
class BeginMessage(MsgPayload):
    pass


@dataclass(frozen=True)
class CommitMessage(MsgPayload):
    pass


@dataclass(frozen=True)
class ChangeMessage(MsgPayload):
    table: str


@dataclass(frozen=True)
class GenericMessage(MsgPayload):
    pass

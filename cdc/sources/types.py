from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import NamedTuple, NewType
from cdc.types import Payload
from cdc.streams.types import StreamMessage

Id = NewType("Id", int)
Position = NewType("Position", int)


class CdcMessage(NamedTuple):
    """
    An abstraction that represents a replication message with a logical id used
    internally to the producer.
    """

    # A sequential identifier which can be used to ensure that messages are
    # being delivered in the correct order. This sequence is initialized to 1
    # at process startup, and doesn't have any particular significance outside
    # of the running process.
    id: Id

    payload: ReplicationEvent


@dataclass(frozen=True)
class ReplicationEvent(ABC):
    """
    An abstraction that represents a replication message received from a source
    and produced to a stream.
    """

    # The current replication position. This value is provided by the source.
    # and the meaning of the value is dependent on which source is being used.
    position: Position

    # The replication data payload as it should be provided to the stream after
    # any source specific processing.
    payload: Payload

    def to_stream(self) -> StreamMessage:
        return StreamMessage(payload=self.payload)


@dataclass(frozen=True)
class BeginMessage(ReplicationEvent):
    pass


@dataclass(frozen=True)
class CommitMessage(ReplicationEvent):
    pass


@dataclass(frozen=True)
class ChangeMessage(ReplicationEvent):
    """
    A DML operation performed on a specific table.
    """

    table: str

    def to_stream(self) -> StreamMessage:
        return StreamMessage(payload=self.payload, metadata={"table": self.table})


@dataclass(frozen=True)
class GenericMessage(ReplicationEvent):
    pass

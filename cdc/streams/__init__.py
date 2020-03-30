import jsonschema  # type: ignore
from typing import Callable, Optional

from cdc.sources.types import ReplicationMessage, ChangeMessage
from cdc.streams.backends import ProducerBackend, producer_registry
from cdc.utils.registry import Configuration


class Producer(object):
    """
    Destination for replication messages.

    This class contains the interfaces to common functionality for all types of
    generic replication message producers. The specific details are delegated
    to the backend implementation.
    """

    def __init__(self, backend: ProducerBackend):
        self.__backend = backend

    def __repr__(self) -> str:
        return "<{type}: {backend}>".format(
            type=type(self).__name__, backend=self.__backend
        )

    def __len__(self) -> int:
        """
        Returns the number of messages currently waiting to be flushed.
        """
        return len(self.__backend)

    def write(self, payload: ReplicationMessage, callback: Callable[[], None]) -> None:
        """
        Write a replication payload to the destination.

        This method should not block. When the message has succesfully been
        flushed, the callback will be invoked without parameters.
        """
        self.__backend.write(payload, callback)

    def poll(self, timeout: float) -> None:
        """
        Invokes any callbacks ready to be invoked. If no callbacks are ready to
        be invoked, waits until one is ready or the timeout is reached,
        whichever comes first.
        """
        self.__backend.poll(timeout)

    def flush(self, timeout: float) -> int:
        """
        Wait for all messages to be flushed or the timeout to be reached,
        whichever comes first.
        Returns the number of messages still in queue, thus not sent.
        """
        return self.__backend.flush(timeout)


def producer_factory(configuration: Configuration) -> Producer:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {
                "backend": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "options": {"type": "object"},
                    },
                    "required": ["type"],
                }
            },
            "required": ["backend"],
        },
    )
    return Producer(
        backend=producer_registry.new(
            configuration["backend"]["type"], configuration["backend"]["options"]
        )
    )

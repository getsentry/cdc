import jsonschema  # type: ignore
from typing import Callable

from cdc.sources.types import Payload
from cdc.streams.backends import ProducerBackend, producer_registry
from cdc.utils.registry import Configuration


class Producer(object):
    def __init__(self, backend: ProducerBackend):
        self.__backend = backend

    def __repr__(self) -> str:
        return "<{type}: {backend}>".format(
            type=type(self).__name__, backend=self.__backend
        )

    def __len__(self) -> int:
        return len(self.__backend)

    def write(self, payload: Payload, callback: Callable[[], None]) -> None:
        self.__backend.write(payload, callback)

    def poll(self, timeout: float) -> None:
        self.__backend.poll(timeout)

    def flush(self, timeout: float) -> None:
        self.__backend.flush(timeout)


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

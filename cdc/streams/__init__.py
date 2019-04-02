import jsonschema
from typing import Callable

from cdc.registry import Configuration
from cdc.sources.types import Payload
from cdc.streams.backends import PublisherBackend, publisher_registry


class Publisher(object):
    def __init__(self, backend: PublisherBackend):
        self.__backend = backend

    def __repr__(self) -> str:
        return "<{type}: {backend}>".format(
            type=type(self).__name__, backend=self.__backend
        )

    def __len__(self) -> int:
        return len(self.__backend)

    def validate(self) -> None:
        self.__backend.validate()

    def write(self, payload: Payload, callback: Callable[[], None]) -> None:
        self.__backend.write(payload, callback)

    def poll(self, timeout: float) -> None:
        self.__backend.poll(timeout)

    def flush(self, timeout: float) -> None:
        self.__backend.flush(timeout)


def publisher_factory(configuration: Configuration) -> Publisher:
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
    return Publisher(
        backend=publisher_registry.new(
            configuration["backend"]["type"], configuration["backend"]["options"]
        )
    )

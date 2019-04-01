import jsonschema
from typing import Callable

from cdc.sources.types import Payload
from cdc.streams.backends import PublisherBackend, registry


class Publisher(object):
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
            }
        },
        "required": ["backend"],
    }

    def __init__(self, configuration):
        jsonschema.validate(configuration, self.schema)

        self.__backend: PublisherBackend = registry[configuration["backend"]["type"]](
            configuration["backend"]["options"]
        )

    def __repr__(self):
        return "<{type}: {backend}>".format(
            type=type(self).__name__, backend=self.__backend
        )

    def __len__(self) -> int:
        return len(self.__backend)

    def validate(self):
        self.__backend.validate()

    def write(self, payload: Payload, callback: Callable[[], None]):
        self.__backend.write(payload, callback)

    def poll(self, timeout: float):
        self.__backend.poll(timeout)

    def flush(self, timeout: float):
        self.__backend.flush(timeout)

import logging
from abc import ABC, abstractmethod
from typing import Mapping, Type

from cdc.common import Message
from cdc.logging import LoggerAdapter


logger = LoggerAdapter(logging.getLogger(__name__))


class PublisherBackend(ABC):
    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    def validate(self):
        logger.trace("Validation is not implemented for %r.", self)

    @abstractmethod
    def write(self, message: Message, callback):  # TODO: type
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: float):
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: float):
        raise NotImplementedError


from cdc.streams.backends.kafka import KafkaPublisherBackend

registry: Mapping[str, Type[PublisherBackend]] = {
    'kafka': KafkaPublisherBackend,
}

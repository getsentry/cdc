import pytest

from cdc.streams.backends.kafka import KafkaProducerBackend
from cdc.types import ConfigurationError


def test_idempotence_required():
    KafkaProducerBackend("topic", {})  # should not raise

    KafkaProducerBackend("topic", {"enable.idempotence": "true"})  # should not raise

    KafkaProducerBackend("topic", {"enable.idempotence": True})  # should not raise

    with pytest.raises(ConfigurationError):
        KafkaProducerBackend("topic", {"enable.idempotence": "false"})

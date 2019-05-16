from datadog import DogStatsd
from cdc.utils.registry import Configuration
import jsonschema  # type: ignore

METRIC_PREFIX = "cdc"

def datadog_factory(configuration: Configuration) -> DogStatsd:
    jsonschema.validate(
        configuration,
        {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer"},
            },
            "required": ["host", "port"],
        },
    )
    return DogStatsd(
        host=configuration["host"], port=configuration["port"],
        namespace=METRIC_PREFIX)


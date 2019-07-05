from datadog import DogStatsd  # type: ignore

import jsonschema  # type: ignore
import logging
import time


from cdc.utils.logging import LoggerAdapter
from cdc.utils.registry import Configuration

logger = LoggerAdapter(logging.getLogger(__name__))


METRIC_PREFIX = "cdc"


class Stats:
    MESSAGE_FLUSHED_METRIC = "message_flushed"
    TASK_EXECUTED_TIME_METRIC = "task_executed"

    def __init__(self, configuration: Configuration) -> None:
        jsonschema.validate(
            configuration,
            {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "message_sampling_rate": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "task_sampling_rate": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                },
                "required": ["host", "port"],
            },
        )
        self.__dogstatsd = DogStatsd(
            host=configuration["host"],
            port=configuration["port"],
            namespace=METRIC_PREFIX,
        )

        self.__message_sampling_rate: float = configuration.get("message_sampling_rate", 1.0)
        self.__task_sampling_rate: float = configuration.get("task_sampling_rate", 1.0)

    def message_flushed(self, start: float) -> None:
        self.__record_simple_interval(
            start, self.MESSAGE_FLUSHED_METRIC, self.__message_sampling_rate
        )

    def task_executed(self, start: float, tasktype: str) -> None:
        tag = "%s:%s" % ("tasktype", tasktype)
        self.__record_simple_interval(
            start, self.TASK_EXECUTED_TIME_METRIC, self.__task_sampling_rate, [tag]
        )

    def __record_simple_interval(
        self, start: float, metric: str, sample_rate: float, tags: list = None
    ) -> None:
        now = time.time()
        duration = int((now - start) * 1000)
        try:
            self.__dogstatsd.timing(
                metric, duration, tags=tags, sample_rate=sample_rate
            )
        except Exception as e:
            logger.exception(e)

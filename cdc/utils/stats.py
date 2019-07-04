from datadog import DogStatsd

import jsonschema
import logging
import time


from cdc.utils.logging import LoggerAdapter

logger = LoggerAdapter(logging.getLogger(__name__))


METRIC_PREFIX = "cdc"


class Stats:
    MESSAGE_FLUSHED_METRIC = "message_flushed"
    TASK_EXECUTED_TIME_METRIC = "task_executed"

    def __init__(self, host: str, port: int, message_sampling_rate: float = 1.0, task_sampling_rate: float = 1.0) -> None:
        self.__dogstatsd = DogStatsd(host=host, port=port, namespace=METRIC_PREFIX)
        self.__message_sampling_rate = message_sampling_rate
        self.__task_sampling_rate = task_sampling_rate

    def message_flushed(self, start: int) -> None:
        self.__record_simple_interval(
            start, self.MESSAGE_FLUSHED_METRIC, self.__message_sampling_rate
        )

    def task_executed(self, start: int, tasktype: str) -> None:
        tag = "%s:%s" % ("tasktype", tasktype)
        self.__record_simple_interval(
            start, self.TASK_EXECUTED_TIME_METRIC, self.__task_sampling_rate, [tag]
        )

    def __record_simple_interval(
        self, start: float, metric: str, sample_rate: int, tags: list = None
    ) -> None:
        now = time.time()
        duration = int((now - start) * 1000)
        try:
            self.__dogstatsd.timing(
                metric, duration, tags=tags, sample_rate=sample_rate
            )
        except Exception as e:
            logger.exception(e)

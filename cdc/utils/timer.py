from datadog import DogStatsd
import time
import logging

from cdc.utils.logging import LoggerAdapter

logger = LoggerAdapter(logging.getLogger(__name__))

class Timer:

    def __init__(self, statsd: DogStatsd) -> None:
        self.__statsd = statsd
        
    def record_simple_interval(self, start: float, metric: str, tag: str = None) -> None:
        now = time.time()
        duration = int((now - start) * 1000)
        try:
            self.__statsd.timing(metric, duration, tags=[tag] if tag else None)
        except Exception as e:
            logger.exception(e)

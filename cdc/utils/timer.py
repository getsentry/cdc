from datadog import DogStatsd
import time
import logging

logger = LoggerAdapter(logging.getLogger(__name__))

class Timer:
    METRIC_KEY = "message_delay"

    def __init__(self, statsd: DogStatsd) -> None:
        self.__statsd = statsd
        self.__start = None

    def init(self) -> None:
        self.__start = time.time()

    def reset(self) -> None:
        self.__start = None

    def finish(self) -> None:
        if not self.__start:
            logger.error("Invalid timer state when calling finish. Timer was not initialized.")
            return
            
        now = time.time()
        duration = int((now - self.__start) * 1000000)
        try:
            self.__statsd.timing(self.METRIC_KEY, duration)
        except Exception as e:
            logger.exception(e)
        finally:
            self.__start = None

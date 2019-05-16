from datadog import DogStatsd
import time
import logging

from cdc.utils.logging import LoggerAdapter

logger = LoggerAdapter(logging.getLogger(__name__))

class Timer:

    MAX_TIMERS = 64

    def __init__(self, key: str, statsd: DogStatsd) -> None:
        self.__key = key
        self.__statsd = statsd
        self.__timers = {}

    def init(self, metric: str, tag: str = None) -> None:
        if len(self.__timers) > self.MAX_TIMERS:
            logger.error("Trying to initialize too many timers.")
            return

        if (metric, tag) in self.__timers:
            logger.warning(
                "Reinitializing existing timer %s - %s without calling reset" % (metric, tag))

        start = time.time()
        self.__timers[(metric, tag)] = start

    def reset(self, metric: str, tag: str = None) -> None:
        if (metric, tag) not in self.__timers:
            logger.error(
                "Invalid timer state when calling reset (%s, %s). Timer was not initialized." % (metric, tag))
        
        del self.__timers[(metric, tag)]

    def isinit(self, metric: str, tag: str = None) -> bool:
        return (metric, tag) in self.__timers

    def finish(self, metric: str, tag: str = None) -> None:
        if (metric, tag) not in self.__timers:
            logger.error(
                "Invalid timer state when calling finish (%s, %s). Timer was not initialized." % (metric, tag))
            return

        start = self.__timers[(metric, tag)]
        self.recordInterval(start, metric, tag)
        self.reset(metric, tag)
        

    def recordInterval(self, start: float, metric: str, tag: str = None) -> None:
        now = time.time()
        duration = int((now - start) * 1000)
        key = "%s.%s" % (self.__key, metric)
        try:
            self.__statsd.timing(key, duration, tags=[tag] if tag else None)
        except Exception as e:
            logger.exception(e)
            
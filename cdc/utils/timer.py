from datadog import DogStatsd
import time

class Timer:
    METRIC_KEY = "message_delay"

    def __init__(self, statsd: DogStatsd):
        self.__statsd = statsd
        self.__start = None

    def init(self):
        self.__start = time.time()

    def reset(self):
        self.__start = None

    def finish(self):
        if self.__start:
            now = time.time()
            duration = int((now - self.__start) * 1000000)
            self.__statsd.timing(self.METRIC_KEY, duration)
            self.__start = None

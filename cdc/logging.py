import logging


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger):
        super(LoggerAdapter, self).__init__(logger, {})

    def trace(self, *args, **kwargs):
        return self.log(5, *args, **kwargs)

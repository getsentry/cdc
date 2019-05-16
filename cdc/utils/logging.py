import logging


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger):
        super(LoggerAdapter, self).__init__(logger, {})

    def trace(self, *args, **kwargs) -> None:
        return self.log(5, *args, **kwargs)

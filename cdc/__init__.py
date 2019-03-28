import logging

logging.addLevelName(5, 'TRACE')

class Logger(logging.getLoggerClass()):
    def trace(self, msg, *args, **kwargs):
        return self.log(5, msg, *args, **kwargs)

logging.setLoggerClass(Logger)

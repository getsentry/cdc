import logging


logging.addLevelName(5, "TRACE")


from datetime import datetime
from typing import NamedTuple, Callable


class ScheduledTask(NamedTuple):
    deadline: datetime
    callable: Callable[[], None]

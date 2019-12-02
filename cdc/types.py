from datetime import datetime
from typing import Callable, NamedTuple


class ConfigurationError(Exception):
    pass


class ScheduledTask(NamedTuple):
    deadline: datetime
    callable: Callable[[], None]
    tasktype: str

    def get_timeout(self, now: datetime) -> float:
        return (self.deadline - now).total_seconds()

    def get_type(self) -> str:
        return self.tasktype

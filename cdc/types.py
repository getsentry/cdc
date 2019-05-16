from datetime import datetime
from typing import Callable, NamedTuple


class ScheduledTask(NamedTuple):
    deadline: datetime
    callable: Callable[[], None]

    def get_timeout(self, now: datetime) -> float:
        return (self.deadline - now).total_seconds()

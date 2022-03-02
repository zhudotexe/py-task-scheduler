import datetime
import functools
import time


@functools.total_ordering
class Task:
    def __init__(self, when: datetime.datetime, func_name: str, *args, **kwargs):
        self.when = when
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs

    def __eq__(self, other):
        return self.when == other.when

    def __lt__(self, other):
        return self.when < other.when

    def is_runnable_now(self) -> bool:
        """Is this task runnable right now?"""
        return self.time_until() <= 0

    def time_until(self) -> float:
        """Returns the time until this task should be run, in seconds."""
        now = time.time()
        return self.when.timestamp() - now


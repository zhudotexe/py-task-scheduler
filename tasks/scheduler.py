import asyncio
import datetime
import inspect
import logging
from typing import Callable, Coroutine, Dict, Optional, Union

from .task import Task

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(self):
        self.tasks: asyncio.PriorityQueue[Task] = asyncio.PriorityQueue()
        self.callbacks: Dict[str, Union[Callable, Coroutine]] = {}
        self.waiting_task: Optional[WaitUntil] = None
        self.save_lock = asyncio.Lock()
        self.running = False

    def start(self):
        """
        Begin running scheduled tasks. Any tasks scheduled for the past will be run immediately.
        This should only be called after all entrypoints have been registered.
        """
        if self.running:
            raise ValueError("Scheduler is already running!")
        self.running = True
        asyncio.create_task(self._main_loop())

    def stop(self):
        """
        Stop running scheduled tasks.
        """
        self.running = False

    def submit(self, when: datetime.datetime, func_name: Union[str, Callable], *args, **kwargs):
        """
        Submit a task to run at some point after the given *when*. All *args* and *kwargs* must be JSON-serializable.
        """
        if not isinstance(func_name, str):
            func_name = func_name.__name__

        task = Task(when, func_name, *args, **kwargs)
        self.tasks.put_nowait(task)
        if self.waiting_task is not None and when.timestamp() < self.waiting_task.target_time:
            self.waiting_task.cancel()

    def task_entrypoint(self, name: str = None):
        """
        Decorate methods that a task is allowed to call with this decorator. If a name is not given, registers an
        entrypoint with the decorated function's name.
        """

        def deco(inner):
            if name is None:
                inner_name = inner.__name__
            else:
                inner_name = name

            if inner_name in self.callbacks:
                raise ValueError(f"Duplicate task entrypoint registration! ({inner_name})")

            self.callbacks[inner_name] = inner
            return inner

        return deco

    # ==== internal ====
    async def _main_loop(self):
        while self.running:
            await self.save_lock.acquire()
            next_task = await self.tasks.get()

            if next_task.is_runnable_now():
                self.save_lock.release()
                await self._run_task(next_task)
            else:
                await self.tasks.put(next_task)
                self.save_lock.release()
                self.waiting_task = WaitUntil(next_task)
                await self.waiting_task
                self.waiting_task = None

    async def _run_task(self, task: Task):
        try:
            entrypoint = self.callbacks[task.func_name]
        except KeyError:
            log.error(f"No task entrypoint named {task.func_name!r} found!")
            return

        # noinspection PyBroadException
        try:
            if inspect.iscoroutinefunction(entrypoint):
                await entrypoint(*task.args, **task.kwargs)
            else:
                entrypoint(*task.args, **task.kwargs)
        except Exception:
            log.exception(f"Task calling into {task.func_name!r} raised an exception on execution!")


class WaitUntil:
    def __init__(self, target_task: Task):
        self.target_time = target_task.when.timestamp()
        self.waiter = asyncio.create_task(asyncio.sleep(target_task.time_until()))

    def __await__(self):
        yield from self.waiter

    def cancel(self):
        self.waiter.cancel()

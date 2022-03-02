import asyncio
import datetime
import inspect
import json
import logging
from typing import Callable, Coroutine, Dict, Optional, TextIO, Union

from .task import Task

log = logging.getLogger(__name__)


class WaitUntil:
    def __init__(self, target_task: Task):
        self.target_time = target_task.when.timestamp()
        self.waiter = asyncio.create_task(asyncio.sleep(target_task.time_until()))

    def __await__(self):
        yield from self.waiter

    def cancel(self):
        self.waiter.cancel()


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


class JSONSavingScheduler(Scheduler):
    def __init__(self, fp):
        super().__init__()
        self.fp = fp

    # ==== save ====
    async def save(self):
        if self.tasks.empty():
            open(self.fp, 'w').close()
            return

        async with self.save_lock:
            dest_file = open(self.fp, 'w', encoding='utf-8')
            new_task_queue = asyncio.PriorityQueue()
            while not self.tasks.empty():
                task = await self.tasks.get()
                await asyncio.get_event_loop().run_in_executor(None, self._write_task, dest_file, task)
                await new_task_queue.put(task)
            dest_file.close()
            self.tasks = new_task_queue

    @staticmethod
    def _write_task(f: TextIO, task: Task):
        data = {
            "when": task.when.timestamp(),
            "func_name": task.func_name,
            "args": task.args,
            "kwargs": task.kwargs
        }
        try:
            line = json.dumps(data)
        except (TypeError, ValueError):
            log.warning(f"Could not save task (make sure the args are JSON-serializable!): {data!r}")
            return
        f.write(line)
        f.write("\n")

    # ==== load ====
    def load(self):
        try:
            with open(self.fp, 'r', encoding='utf-8') as f:
                for line in f:
                    self._load_line(line)
        except FileNotFoundError:
            log.info(f"Task save file {self.fp!r} not found, no tasks loaded.")
            return

    def _load_line(self, line: str):
        try:
            data = json.loads(line)
            self.submit(
                datetime.datetime.fromtimestamp(data['when']),
                str(data['func_name']),
                *data['args'],
                **data['kwargs']
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            log.warning(f"Invalid data found during task load, skipping: {line!r}")

# py-task-scheduler

Schedule tasks to run at a given time.

## Usage Example

```py
import asyncio
import datetime

from tasks import JSONSavingScheduler

scheduler = JSONSavingScheduler("tasks.jsonl")
scheduler.load()


@scheduler.task_entrypoint()
async def do_thing(foo: int):
    print(foo)


async def main():
    scheduler.submit(datetime.datetime.now() + datetime.timedelta(seconds=5), do_thing, foo=1)
    scheduler.start()
    try:
        await asyncio.sleep(100)
    finally:
        await scheduler.save()


if __name__ == '__main__':
    asyncio.run(main())

```
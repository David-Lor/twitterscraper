import asyncio
import datetime
import pytimeparse
import ujson as json
import dateutil.parser
from bs4 import BeautifulSoup
from typing import Coroutine

__all__ = [
    "json", "datetime",
    "get_datetime_now", "parse_datetime", "parse_duration_to_seconds", "html_parse", "sleep_event",
    "AsyncPool"
]


def get_datetime_now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


def parse_duration_to_seconds(duration_str: str) -> int | float:
    return pytimeparse.parse(duration_str)


def parse_datetime(dt_str: str, **kwargs) -> datetime.datetime:
    return dateutil.parser.parse(dt_str, **kwargs)


def html_parse(src: str) -> BeautifulSoup:
    return BeautifulSoup(src, features="lxml")


async def sleep_event(event: asyncio.Event, timeout: float):
    try:
        await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
        pass


class AsyncPool:
    def __init__(self, concurrency_limit: int):
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.tasks: list[Coroutine] = list()
        self.results: list = list()

    def add_task(self, coro: Coroutine):
        self.tasks.append(coro)

    async def run(self):
        self.results = [None] * len(self.tasks)
        await asyncio.gather(*[self._run_one(i, coro) for i, coro in enumerate(self.tasks)])

    async def _run_one(self, idx: int, coro: Coroutine):
        async with self.semaphore:
            self.results[idx] = await coro

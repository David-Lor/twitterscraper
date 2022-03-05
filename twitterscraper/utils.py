import time
import uuid
import asyncio
import functools
from typing import *

import datetime


def daterange(start_date: datetime.date, end_date: Optional[datetime.date] = None, end_inclusive: bool = True):
    """Iterate each day between two dates, yielding each found day.
    If end_date is None, use current date.
    If end_inclusive=True, yield the last day.
    """
    if end_date is None:
        end_date = datetime.date.today()
    if end_inclusive:
        end_date += datetime.timedelta(days=1)

    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def date_to_datetime_range(start_date: datetime.date, end_datetime: datetime.datetime):
    """Iterate each day between a datetime.date until a datetime.datetime, yielding each range's datetime.datetime
    (the start is inclusive, the end is exclusive). The final yield corresponds to the partial time on the end_datetime.
    Everything is UTC.
    """
    end_date = end_datetime.date()
    for date in daterange(start_date=start_date, end_date=end_date, end_inclusive=False):
        from_dt = date_to_datetime(date)
        to_dt = from_dt + datetime.timedelta(days=1)
        yield from_dt, to_dt

    if not end_datetime.time().isoformat().startswith("00:00:00"):
        end_day_datetime = date_to_datetime(end_datetime.date())
        yield end_day_datetime, end_datetime


def date_to_datetime(date: datetime.date) -> datetime.datetime:
    """Convert a datetime.date object to datetime.datetime, set at 00:00h UTC."""
    return datetime.datetime(date.year, date.month, date.day, tzinfo=datetime.timezone.utc)


def day_to_timestamps(date: datetime.date) -> Tuple[int, int]:
    """Given a datetime.date, return the Unix seconds UTC timestamps, for the (start, end) of the day.
    start is inclusive, end is exclusive."""
    # new datetime objects are supposed to be considering the current timezone
    tomorrow = date + datetime.timedelta(days=1)
    start_dt = datetime.datetime(date.year, date.month, date.day)
    end_dt = datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def date_to_timestamp(date: datetime.date) -> int:
    """Given a datetime.date, return the Unix seconds UTC timestamp of that date at 00:00h UTC."""
    # TODO currently no timezone validation
    dt = datetime.datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=datetime.timezone.utc)
    return datetime_to_timestamp(dt)


def datetime_to_timestamp(dt: datetime.datetime) -> int:
    """Given a datetime.datetime, return the Unix seconds UTC timestamp.
    If the datetime object does not have a timezone, consider the current host timezone."""
    # TODO currently no timezone validation
    return int(dt.timestamp())


def timestamp_to_datetime(timestamp: int) -> datetime.datetime:
    """Given a Unix seconds UTC timestamp, return a datetime.datetime object, set to the UTC timezone."""
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def datetime_to_twitter_isoformat(dt: datetime.datetime) -> str:
    """Given a datetime.datetime, that must be on UTC timezone, return the isoformat used by Twitter (ending with Z)."""
    if dt.tzinfo != datetime.timezone.utc:
        raise ValueError("datetime not with UTC timezone")
    isoformat = dt.isoformat()
    isoformat = isoformat.replace("+00:00", "Z")
    if not isoformat.endswith("Z"):
        isoformat += "Z"
    return isoformat


def timestamp_in_range(from_ts: int, to_ts: int, ts: int) -> bool:
    return from_ts <= ts < to_ts


def get_timestamp() -> int:
    """Get the current time as Unix seconds UTC timestamp"""
    return int(time.time())


def get_uuid():
    return str(uuid.uuid4())


def async_entrypoint(async_func):
    """Decorator required for entrypoint functions (after the Typer app decorator).
    Allows running async functions as entrypoints."""
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_func(*args, **kwargs))
    return functools.update_wrapper(wrapper, async_func)


class Singleton:
    @classmethod
    def get(cls):
        """Get the current class singleton instance. If not set raises AttributeError."""
        # noinspection PyUnresolvedReferences
        return cls._instance

    @classmethod
    def set(cls, instance):
        """Set an instance as the class singleton instance."""
        cls._instance = instance
        return cls.get()

    def set_singleton(self):
        """Set the current instance as the class singleton instance."""
        self.__class__.set(self)
        return self

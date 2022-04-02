import datetime
import os
from typing import *

import pydantic
import yaml

import twitterscraper.utils


SETTINGS_FILE = os.getenv("SETTINGS_FILE", "settings.yaml")


class AMQPSettings(pydantic.BaseModel):
    class Queues(pydantic.BaseModel):
        class QueueConfig(pydantic.BaseModel):
            name: str
            """Queue name"""
            persistent: bool = True
            """If persistent, enqueue messages with Persistent mode (deliveryMode=2)"""
            workers: int = 10
            """Amount of concurrent messages that can be consumed, thus effective parallel works that can be handled.
            This sets the channel QoS prefetch_count when consuming"""

        # Queues
        fetchpersist: QueueConfig
        """Queue for Fetch & Persist jobs"""
        persistedreview: QueueConfig
        """Queue for Persisted Review jobs"""

    # AMQPSettings
    uri: pydantic.AnyUrl
    """AMQP connection URI"""
    queues: Queues
    """Queues for each job type"""
    exchange: str = ""
    """Exchange to use when consuming"""


class TwitterSettings(pydantic.BaseModel):
    class Keys(pydantic.BaseModel):
        key: str
        """API Key"""
        secret: str
        """API Secret Key"""
        token: str
        """API Bearer Token"""

    # TwitterSettings
    keys: Keys
    """Keys for using the Twitter API"""
    nitter_baseurl: List[pydantic.AnyHttpUrl] = pydantic.Field(default="https://nitter.net", min_items=1)
    """Base URL of a Nitter instance, used for scraping purposes. A list of URLs can be given, in which case
    a random instance will be picked each time"""

    @pydantic.validator("nitter_baseurl", pre=True)
    def _nitter_baseurl_string_to_list(cls, v):
        if isinstance(v, str):
            v = [v]
        return v


class TasksSettings(pydantic.BaseModel):
    class Task(pydantic.BaseModel):
        period: Optional[datetime.timedelta]
        """Run the task periodically. The time can be given:
        - as seconds
        - on human-legible format (examples: "30s", "30m", "12h30m", "1d", "1d5h30s")
        - on ISO 8601 duration format
        If period is given, any other schedule method is ignored.
        """
        period_start_now: bool = True
        """For Period schedule method, run the task at the beginning; otherwise, wait `period` until the first run."""
        daily_times: Optional[List[datetime.time]]
        """Run the task at the given times (like "12:25:00" or "12:25"), everyday."""

        @property
        def period_seconds(self) -> Optional[int]:
            if self.period is not None:
                return int(self.period.total_seconds())

        @pydantic.validator("period", pre=True)
        def _parse_period(cls, v):
            """If the period is given as string, try to convert it to ISO 8601 duration format.
            Accepts durations like "1d2h3m4s"
            https://pydantic-docs.helpmanual.io/usage/types/#datetime-types"""
            if not isinstance(v, str):
                return v
            v = v.upper()
            if not v.startswith("P"):
                v = "P" + v
            if "D" in v and "DT" not in v:
                v = v.replace("D", "DT")
            if "DT" not in v:
                v = v.replace("P", "PT")
            return v

        @pydantic.validator("daily_times", pre=True)
        def _parse_arrays(cls, v):
            """If a list attribute is given as string, try to convert it to list splitting by commas"""
            if not isinstance(v, str):
                return v
            chunks = v.split(",")
            chunks = [c.strip() for c in chunks]
            return chunks

    # TasksSettings
    new_tweets_scan: Task
    persisted_tweets_scan: Task


class PersistenceSettings(pydantic.BaseModel):
    uri: pydantic.AnyUrl
    save_jobs: bool = False


class MainSettings(pydantic.BaseModel, twitterscraper.utils.Singleton):
    get: ClassVar[Callable[..., "MainSettings"]]
    amqp: AMQPSettings
    twitter: TwitterSettings
    persistence: PersistenceSettings
    tasks: TasksSettings


def load_settings() -> MainSettings:
    """Load the settings file and initialize the MainSettings, singleton-setting it, and return it on the current call.
    Must be called on app setup."""
    with open(SETTINGS_FILE, "r") as f:
        data = f.read()
    data = yaml.safe_load(data)

    settings = MainSettings(**data)
    MainSettings.set(settings)
    return settings

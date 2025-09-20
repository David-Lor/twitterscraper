import os
import time
import random
import pathlib
import yaml
import pydantic
from pydantic.functional_validators import BeforeValidator
from typing_extensions import Annotated
from .utils import parse_duration_to_seconds, parse_cron

SETTINGS_FILE = os.getenv("SETTINGS_FILE", "settings.yaml")

Cron = Annotated[str, BeforeValidator(parse_cron)]
Duration = Annotated[float, BeforeValidator(parse_duration_to_seconds)]


class User(pydantic.BaseModel):
    username: str
    alias: str | None = None

    def get_alias_or_username(self):
        return self.alias or self.username


class UserPassword(pydantic.BaseModel):
    user: str
    password: pydantic.SecretStr


class Timer(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    delay: Duration | None = None
    jitter: Duration | None = None
    cron: str | None = None

    def get_delay_with_jitter(self) -> float:
        diff = 0
        if self.jitter:
            diff = random.uniform(-self.jitter, self.jitter)

        delay = self.get_delay_to_next_cron() if self.cron else self.delay
        delay_with_jitter = delay + diff
        if delay_with_jitter < 0:
            delay_with_jitter = delay

        return delay_with_jitter

    def get_delay_to_next_cron(self) -> float:
        next_epoch = parse_cron(self.cron).get_next()
        now_epoch = time.time()
        return next_epoch - now_epoch

    @pydantic.model_validator(mode="after")
    @classmethod
    def _validate(cls, data: "Timer"):
        if not data.cron and not data.delay:
            raise ValueError("No delay or cron specified")
        return data


class Schedule(Timer):
    enabled: bool = True
    initial: Timer | None = None

    def get_initial_delay(self):
        if self.cron:
            return self.get_delay_to_next_cron()
        if self.initial:
            return self.initial.get_delay_with_jitter()
        return 0


class ScheduleWithConcurrency(Schedule):
    concurrency_limit: int = 10


class ScheduleWithConcurrencyDisabled(ScheduleWithConcurrency):
    enabled: bool = False


class HttpSettings(pydantic.BaseModel):
    proxies: list[str] | None = None
    timeout: Duration = 10

    @classmethod
    def with_default_timeout(cls, default_timeout: Duration):
        class HttpSettingsMod(cls):
            timeout: Duration = default_timeout
        return HttpSettingsMod


class ScraperSettings(pydantic.BaseModel):
    users: list[User]
    http: HttpSettings = pydantic.Field(default_factory=HttpSettings)


class TweetDeletedSchedulerSettings(ScheduleWithConcurrency):

    class EnsureDeleted(ScheduleWithConcurrencyDisabled):
        retries: int = 10

    ensure_deleted: EnsureDeleted | None = None


class SchedulersSettings(pydantic.BaseModel):
    tweet_scraper: Schedule
    tweet_deleted: TweetDeletedSchedulerSettings
    tweet_deleted_recheck: TweetDeletedSchedulerSettings
    tweet_archiver: ScheduleWithConcurrencyDisabled


class ArchiversSettings(pydantic.BaseModel):

    class ArchiveToday(pydantic.BaseModel):
        http: HttpSettings = pydantic.Field(default_factory=HttpSettings.with_default_timeout(300))

    # ArchiversSettings
    archivetoday: ArchiveToday = pydantic.Field(default_factory=ArchiveToday)


class PersistenceSettings(pydantic.BaseModel):

    class MongoDB(pydantic.BaseModel):

        class Collections(pydantic.BaseModel):
            user_tweets: str = "tweets"
            kv: str = "kv"

        # MongoDB
        uri: pydantic.MongoDsn
        authentication: UserPassword | None = None
        database: str = "twitterscraper"
        collections: Collections = pydantic.Field(default_factory=Collections)

        @property
        def uri_with_auth(self) -> str:
            uri = str(self.uri)
            if self.authentication:
                uri = uri.replace("%7Buser%7D", self.authentication.user)
                uri = uri.replace("%7Bpassword%7D", self.authentication.password.get_secret_value())

            return uri

    # PersistenceSettings
    mongodb: MongoDB
    concurrency_limit: int = 10


class LogSettings(pydantic.BaseModel):

    class Stdout(pydantic.BaseModel):
        enabled: bool = True
        level: str = "INFO"
        format: str | None = None

    class File(pydantic.BaseModel):
        enabled: bool = False
        level: str = "DEBUG"
        path: pathlib.Path

    # LogSettings
    stdout: Stdout = pydantic.Field(default_factory=Stdout)
    file: File = pydantic.Field(default_factory=File)


class Settings(pydantic.BaseModel):
    scraper: ScraperSettings
    persistence: PersistenceSettings
    schedulers: SchedulersSettings
    archivers: ArchiversSettings = pydantic.Field(default_factory=ArchiversSettings)
    log: LogSettings = pydantic.Field(default_factory=LogSettings)

    @classmethod
    def load_from_file(cls):
        with open(SETTINGS_FILE, "r") as f:
            data = yaml.safe_load(f)

        parsed = cls.model_validate(data)
        cls.__singleton__ = parsed
        return cls.get()

    @classmethod
    def get(cls):
        return cls.__singleton__


def validate_cron(cron: str):
    parse_cron(cron)
    return cron

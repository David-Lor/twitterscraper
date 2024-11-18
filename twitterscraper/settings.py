import os
import random
import yaml
import pydantic
from pydantic.functional_validators import BeforeValidator
from typing_extensions import Annotated
from .utils import parse_duration_to_seconds

SETTINGS_FILE = os.getenv("SETTINGS_FILE", "settings.yaml")

Duration = Annotated[float, BeforeValidator(parse_duration_to_seconds)]


class User(pydantic.BaseModel):
    username: str
    alias: str | None = None

    def get_alias_or_username(self):
        return self.alias or self.username


class UserPassword(pydantic.BaseModel):
    user: str
    password: pydantic.SecretStr


class BaseSchedule(pydantic.BaseModel):
    delay: Duration
    jitter: Duration | None = None

    def get_delay_with_jitter(self):
        diff = 0
        if self.jitter:
            diff = random.uniform(-self.jitter, self.jitter)

        delay = self.delay + diff
        if delay < 0:
            delay = self.delay

        return delay


class Schedule(BaseSchedule):
    enabled: bool = True
    initial: BaseSchedule | None = None


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


class Settings(pydantic.BaseModel):
    scraper: ScraperSettings
    persistence: PersistenceSettings
    schedulers: SchedulersSettings
    archivers: ArchiversSettings = pydantic.Field(default_factory=ArchiversSettings)

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

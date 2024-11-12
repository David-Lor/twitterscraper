import datetime
import pydantic
from .twitter import Tweet


class PartialPersistTweetDeleted(pydantic.BaseModel):
    when_deleted: datetime.datetime | None = None


class TweetArchive(pydantic.BaseModel):
    archiver: str
    url: str
    archived_on: datetime.datetime


class PersistTweet(Tweet, PartialPersistTweetDeleted):
    archives: list[TweetArchive] = []

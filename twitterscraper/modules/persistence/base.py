import abc
import datetime
from typing import AsyncGenerator
from ...models.twitter import Tweet


class BaseRepository(abc.ABC):

    @abc.abstractmethod
    async def save_tweets(self, *tweets: Tweet):
        pass

    @abc.abstractmethod
    def iterate_all_tweets_ids(self, exclude_deleted: bool = False, exclude_archived: bool = False) -> AsyncGenerator[int, None]:
        # NOTE: Typed as def, but implemented as async
        pass

    @abc.abstractmethod
    async def get_tweet(self, tweet_id: int) -> Tweet | None:
        pass

    @abc.abstractmethod
    async def mark_tweet_deleted(self, tweet_id: int, deleted_on: datetime.datetime):
        pass

    @abc.abstractmethod
    async def mark_tweet_archived(self, tweet_id: int, archiver_name: str, archive_url: str, archive_time: datetime.datetime):
        pass

    @abc.abstractmethod
    async def get_twitter_cookies(self) -> list[dict]:
        pass

    @abc.abstractmethod
    async def get_useragents(self) -> list[str]:
        pass

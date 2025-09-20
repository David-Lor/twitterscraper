import abc
from ...models.twitter import Tweet
from ...logger import logger


class BaseTwitterScraper(abc.ABC):

    @abc.abstractmethod
    async def get_user_tweets(self, username: str) -> list[Tweet]:
        pass

    @abc.abstractmethod
    async def tweet_exists(self, tweet_id: int | str) -> bool:
        pass

    @property
    def logger(self):
        return logger.bind(service=self.__class__.__name__)

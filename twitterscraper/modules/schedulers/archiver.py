import httpx

from .base import BaseScheduler
from ..archive.archivetoday import ArchiveToday
from ..persistence.bootstrap import Repository
from ..persistence.base import TweetFilters
from ...settings import Settings
from ...utils import AsyncPool
from ...logger import logger


class TweetArchiver(BaseScheduler):

    def __init__(self):
        self.schedule = Settings.get().schedulers.tweet_archiver
        super().__init__(self.schedule)
        self.archiver = ArchiveToday()

    async def run_loop(self):
        archiver_pool = AsyncPool(self.schedule.concurrency_limit)

        filters = TweetFilters(deleted=False, archived=False)
        async for tweet_id in Repository.get().iterate_all_tweets_ids(filters):
            archiver_pool.add_task(self._task_archive_tweet(tweet_id))

        await archiver_pool.run()

    async def _task_archive_tweet(self, tweet_id: int):
        with logger.contextualize(tweet_id=tweet_id):
            # noinspection PyBroadException
            try:
                tweet = await Repository.get().get_tweet(tweet_id)
                if not tweet:
                    logger.warning("Tweet not found")
                    return

                archived_url, archived_datetime = await self.archiver.get_or_save(tweet.url)
                logger.bind(tweet_username=tweet.user.username, archived_url=archived_url).info("Tweet archived")

                await Repository.get().mark_tweet_archived(
                    tweet_id=tweet.tweet_id,
                    archiver_name=self.archiver.archiver_name,
                    archive_url=archived_url,
                    archive_time=archived_datetime,
                )

            except httpx.HTTPStatusError as error:
                logger.bind(http_statuscode=error.response.status_code).error("Error archiving tweet (HTTPStatusError)")

            except Exception:
                logger.exception("Error archiving tweet")

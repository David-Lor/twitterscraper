from .base import BaseScheduler
from ..archive.archivetoday import ArchiveToday
from ..persistence.bootstrap import Repository
from ...settings import Settings
from ...utils import AsyncPool


class TweetArchiver(BaseScheduler):

    def __init__(self):
        self.schedule = Settings.get().schedulers.tweet_archiver
        super().__init__(self.schedule)
        self.archiver = ArchiveToday()

    async def run_loop(self):
        print(self.scheduler_name, "Running")
        archiver_pool = AsyncPool(self.schedule.concurrency_limit)

        async for tweet_id in Repository.get().iterate_all_tweets_ids(exclude_deleted=True, exclude_archived=True):
            archiver_pool.add_task(self._task_archive_tweet(tweet_id))

        await archiver_pool.run()

    async def _task_archive_tweet(self, tweet_id: int):
        try:
            tweet = await Repository.get().get_tweet(tweet_id)
            if not tweet:
                print("TWEET NOT FOUND", tweet_id)
                return

            archived_url, archived_datetime = await self.archiver.get_or_save(tweet.url)
            print("Archived tweet", tweet.tweet_id, "from", tweet.user.username, ": Saving to DB")
            await Repository.get().mark_tweet_archived(
                tweet_id=tweet.tweet_id,
                archiver_name=self.archiver.archiver_name,
                archive_url=archived_url,
                archive_time=archived_datetime,
            )

        except Exception as ex:
            print(self.__class__.__name__, "ERROR", tweet_id, ex.__class__.__name__, ex)

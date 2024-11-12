import datetime
from .base import BaseScheduler
from ..persistence.bootstrap import Repository
from ..twitter.scraper_syndication import SyndicationTwitterScraper
from ...settings import User, Settings
from ...utils import get_datetime_now, AsyncPool


class TwitterUserScraper(BaseScheduler):
    def __init__(self, user: User):
        self.user = user
        super().__init__(Settings.get().schedulers.tweet_scraper)

    async def run_loop(self):
        print(self.scheduler_name, "Running")
        results = await SyndicationTwitterScraper().get_user_tweets(self.user.username)
        if results is not None:
            print(self.scheduler_name, "Found", len(results), "tweets")
            await Repository.get().save_tweets(*results.values())

    @property
    def scheduler_name(self):
        return self.__class__.__name__ + "-" + self.user.get_alias_or_username()


class DeletedTweetsScraper(BaseScheduler):
    TweetExistsResult = tuple[int, datetime.datetime]

    def __init__(self):
        self.schedule = Settings.get().schedulers.tweet_deleted
        super().__init__(self.schedule)
        self.scraper = SyndicationTwitterScraper()

    async def run_loop(self):
        repository = Repository.get()
        scraper_pool = AsyncPool(concurrency_limit=self.schedule.concurrency_limit)
        updater_pool = AsyncPool(concurrency_limit=Settings.get().persistence.concurrency_limit)

        async for tweet_id in repository.iterate_all_tweets_ids(exclude_deleted=True):
            scraper_pool.add_task(self._task_check_tweet(tweet_id))

        await scraper_pool.run()
        for tweet_id, deleted_on in scraper_pool.results:
            if deleted_on:
                updater_pool.add_task(repository.mark_tweet_deleted(tweet_id, deleted_on))

        await updater_pool.run()

    async def _task_check_tweet(self, tweet_id: int) -> TweetExistsResult | None:
        try:
            print("Checking existence of tweet", tweet_id)
            exists = await self.scraper.tweet_exists(tweet_id)
            print("Tweet", tweet_id, "exists" if exists else "HAS BEEN DELETED")
            return tweet_id, (None if exists else get_datetime_now())
        except Exception as ex:
            print(self.__class__.__name__, "ERROR", tweet_id, ex.__class__.__name__, ex)

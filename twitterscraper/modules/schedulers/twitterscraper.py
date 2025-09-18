import asyncio
import datetime
from .base import BaseScheduler
from ..persistence.bootstrap import Repository
from ..persistence.base import TweetFilters
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
    TweetExistsResult = tuple[int, datetime.datetime | None]
    _tweet_filters = TweetFilters(deleted=False)

    def __init__(self):
        self.schedule = Settings.get().schedulers.tweet_deleted
        super().__init__(self.schedule)
        self.scraper = SyndicationTwitterScraper()

    async def run_loop(self):
        repository = Repository.get()
        scraper_pool, updater_pool = await self.get_scraper_updater_pools()

        async for tweet_id in repository.iterate_all_tweets_ids(self._tweet_filters):
            scraper_pool.add_task(self._task_check_tweet(tweet_id))

        await scraper_pool.run()
        for tweet_id, deleted_on in scraper_pool.results:
            if deleted_on:
                updater_pool.add_task(repository.mark_tweet_deleted(tweet_id, deleted_on))

        await updater_pool.run()

    async def _task_check_tweet(self, tweet_id: int) -> TweetExistsResult:
        tries = 1
        ensure_settings = self.schedule.ensure_deleted
        when_deleted = None
        if ensure_settings and ensure_settings.enabled:
            tries += ensure_settings.retries

        print("Checking existence of tweet", tweet_id)
        for i in range(tries):
            if i > 0:
                delay = ensure_settings.get_delay_with_jitter()
                print("Tweet", tweet_id, "Wait for", delay, "s before checking again")
                await asyncio.sleep(delay)

            try:
                exists = await self.scraper.tweet_exists(tweet_id)
                if exists:
                    print("Tweet", tweet_id, "exists")
                    return tweet_id, None

                print("Tweet", tweet_id, "may have been DELETED")
                if not when_deleted:
                    when_deleted = get_datetime_now()

            except Exception as ex:
                print(self.__class__.__name__, "ERROR", tweet_id, ex.__class__.__name__, ex)
                return tweet_id, None

        # Retries exceeded, consider Deleted
        print("Tweet", tweet_id, "identified as DELETED after", tries, "checks")
        return tweet_id, when_deleted

    async def get_scraper_updater_pools(self):
        scraper_pool = AsyncPool(concurrency_limit=self.schedule.concurrency_limit)
        updater_pool = AsyncPool(concurrency_limit=Settings.get().persistence.concurrency_limit)
        return scraper_pool, updater_pool


class RecheckDeletedTweetsScraper(DeletedTweetsScraper):
    """Check persisted tweets marked as deleted to detect false positives (tweets detected as deleted but still exists),
    in which case their deleted mark is removed.
    """
    _tweet_filters = TweetFilters(deleted=True)

    def __init__(self):
        super().__init__()
        self.schedule = Settings.get().schedulers.tweet_deleted_recheck

    async def run_loop(self):
        repository = Repository.get()
        scraper_pool, updater_pool = await self.get_scraper_updater_pools()

        async for tweet_id in repository.iterate_all_tweets_ids(self._tweet_filters):
            scraper_pool.add_task(self._task_check_tweet(tweet_id))

        await scraper_pool.run()
        for tweet_id, deleted_on in scraper_pool.results:
            if not deleted_on:
                updater_pool.add_task(repository.unmark_tweet_deleted(tweet_id))

        await updater_pool.run()

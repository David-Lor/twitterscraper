import asyncio

from twitterscraper.modules.persistence.bootstrap import Repository
from twitterscraper.modules.schedulers.twitterscraper import TwitterUserScraper, DeletedTweetsScraper
from twitterscraper.modules.schedulers.archiver import TweetArchiver
from twitterscraper.modules.schedulers.base import BaseScheduler
from twitterscraper.settings import Settings


async def amain():
    settings = Settings.load_from_file()
    print("Settings:", settings.model_dump_json(indent=2))

    await Repository.setup()
    schedulers: list[BaseScheduler] = [TwitterUserScraper(user) for user in settings.scraper.users]
    schedulers.append(DeletedTweetsScraper())
    schedulers.append(TweetArchiver())
    await asyncio.gather(*[sched.run() for sched in schedulers])


asyncio.run(amain())

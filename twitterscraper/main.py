import asyncio

from twitterscraper.modules.persistence.bootstrap import Repository
from twitterscraper.modules.schedulers.twitterscraper import TwitterUserScraper, DeletedTweetsScraper, RecheckDeletedTweetsScraper
from twitterscraper.modules.schedulers.archiver import TweetArchiver
from twitterscraper.modules.schedulers.base import BaseScheduler
from twitterscraper.settings import Settings
from twitterscraper.logger import logger, setup_loggers


async def amain():
    settings = Settings.load_from_file()
    setup_loggers(settings.log)
    logger.info("Settings: " + settings.model_dump_json())

    await Repository.setup()
    schedulers: list[BaseScheduler] = [
        TweetArchiver(),
        DeletedTweetsScraper(),
        RecheckDeletedTweetsScraper()
    ]
    schedulers.extend(TwitterUserScraper(user) for user in settings.scraper.users)
    await asyncio.gather(*[sched.run() for sched in schedulers])


def main():
    asyncio.run(amain())

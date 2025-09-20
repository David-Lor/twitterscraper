import asyncio

from .modules.persistence.bootstrap import Repository
from .modules.schedulers.twitterscraper import TwitterUserScraper, DeletedTweetsScraper, RecheckDeletedTweetsScraper
from .modules.schedulers.archiver import TweetArchiver
from .modules.schedulers.base import BaseScheduler
from .settings import Settings
from .logger import logger, setup_loggers
from .utils import get_id


async def amain():
    settings = Settings.load_from_file()

    setup_loggers(settings.log)
    execution_id = get_id()
    with logger.contextualize(execution_id=execution_id):
        logger.bind(settings=settings.model_dump(mode="json"))\
            .info("Loaded settings")

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

import asyncio
import contextlib
from typing import *

import typer

import twitterscraper.controllers.creator
import twitterscraper.controllers.fetchandpersist
import twitterscraper.controllers.persistedreview
import twitterscraper.controllers.tasks_scanners
import twitterscraper.controllers.system
from twitterscraper.settings import MainSettings, load_settings
from twitterscraper.services import Repository, AMQPClient, TwitterAPIClient, TwitterNitterClient
from twitterscraper.utils import async_entrypoint


app = typer.Typer()


@app.command()
@async_entrypoint
async def creator(username: str):
    async with setup_teardown():
        await twitterscraper.controllers.creator.run_creator(username)


@app.command()
@async_entrypoint
async def worker_fetchandpersist(workers: Optional[int] = None):
    async with setup_teardown():
        if workers is not None:
            MainSettings.get().amqp.queues.fetchpersist.workers = workers
        await twitterscraper.controllers.fetchandpersist.run_fetchandpersist_worker()


@app.command()
@async_entrypoint
async def worker_fetchandpersist_once():
    async with setup_teardown():
        await twitterscraper.controllers.fetchandpersist.run_fetchandpersist_worker(msg_limit=1)


@app.command()
@async_entrypoint
async def worker_persistedreview(workers: Optional[int] = None):
    async with setup_teardown():
        if workers is not None:
            MainSettings.get().amqp.queues.persistedreview.workers = workers
        await twitterscraper.controllers.persistedreview.run_persistedreview_worker()


@app.command()
@async_entrypoint
async def worker_persistedreview_once():
    async with setup_teardown():
        await twitterscraper.controllers.persistedreview.run_persistedreview_worker(msg_limit=1)


@app.command()
@async_entrypoint
async def worker_archiveorg():
    async with setup_teardown():
        pass


@app.command()
@async_entrypoint
async def task_newtweetsscan():
    async with setup_teardown():
        await twitterscraper.controllers.tasks_scanners.run_task_newtweetsscan()


@app.command()
@async_entrypoint
async def task_syncprofilestweets():
    async with setup_teardown():
        await twitterscraper.controllers.tasks_scanners.run_task_syncprofilestweets()


@app.command()
@async_entrypoint
async def verify_deletedtweets():
    async with setup_teardown():
        import twitterscraper.controllers.verify_deletedtweets
        await twitterscraper.controllers.verify_deletedtweets.run_verify_deletedtweets()


@app.command()
def db_migrate():
    twitterscraper.controllers.system.db_migrate()


@app.command()
def db_generate_migration(name: Optional[str] = None):
    if not name:
        name = input("Enter migration name: ")
    if len(name) < 3:
        raise ValueError("Migration name empty or too short")
    twitterscraper.controllers.system.db_generate_migration(name)


@contextlib.asynccontextmanager
async def setup_teardown():
    try:
        await setup()
        yield
    finally:
        await teardown()


async def setup():
    settings = load_settings()
    twitter_keys = settings.twitter.keys

    TwitterAPIClient(
        api_key=twitter_keys.key,
        api_secret=twitter_keys.secret,
        api_token=twitter_keys.token,
    ).set_singleton()
    TwitterNitterClient(baseurls=settings.twitter.nitter_baseurl, timeout=settings.twitter.nitter_timeout).set_singleton()
    Repository(uri=settings.persistence.uri).set_singleton()
    AMQPClient(uri=settings.amqp.uri).set_singleton()

    await asyncio.gather(
        AMQPClient.get().connect(),
        Repository.get().tcp_wait_async()
    )


async def teardown():
    await asyncio.gather(
        AMQPClient.get().close(),
        Repository.get().close()
    )


def main():
    app()

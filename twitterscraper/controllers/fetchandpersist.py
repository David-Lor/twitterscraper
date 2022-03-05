import asyncio
import json

from aioify import aioify

from twitterscraper.services.persistence import Repository
from twitterscraper.services.twitter import TwitterNitterClient
from twitterscraper.services.databus import AMQPClient
from twitterscraper.models.jobs import FetchPersistJob
from twitterscraper.settings import MainSettings, AMQPSettings
import twitterscraper.controllers.jobs


async def run_fetchandpersist_worker(msg_limit=None):
    amqp_settings: AMQPSettings = MainSettings.get().amqp
    queue_settings = amqp_settings.queues.fetchpersist
    await AMQPClient.get().consume(
        queue=queue_settings.name,
        callback=_fetchandpersist_job_callback,
        workers=queue_settings.workers,
        msg_limit=msg_limit
    )


async def enqueue_fetchandpersist_jobs(*jobs: FetchPersistJob):
    settings: AMQPSettings = MainSettings.get().amqp
    queue_settings = settings.queues.fetchpersist
    await twitterscraper.controllers.jobs.enqueue_jobs(
        *jobs,
        exchange=settings.exchange,
        routingkey=queue_settings.name,
        persistent=queue_settings.persistent
    )


async def save_profile_last_scan_timestamp(userid: str, timestamp: int):
    print("Updating profile", userid, "lastscantimestamp:", timestamp)
    repository = Repository.get()
    async with repository.session_async():
        profile = await repository.get_profile_by_userid_async(userid)
        profile.last_scan_timestamp = timestamp
        await repository.save_object_async(profile)


async def _fetchandpersist_job_callback(payload: bytes):
    payload_js = json.loads(payload)
    job = FetchPersistJob(**payload_js)
    repository = Repository.get()
    async with repository.session_async() as session:
        profile = await repository.get_profile_by(userid=job.userid)
        tweets = await TwitterNitterClient.get().get_tweets_in_range(
            username=profile.username,
            from_timestamp=job.from_timestamp,
            to_timestamp=job.to_timestamp
        )

        print(f"Persisting {len(tweets)} tweets...")
        failed_persist_tweets = list()

        # We need to set the whole TwitterProfile object on each TwitterTweet object
        # Commit here to set a checkpoint; each tweet saved is commited/rolledback individually,
        # so error persisting an individual tweet does not affect the rest.
        await aioify(session.commit)()

        for tweet in tweets:
            try:
                tweet.profile = profile
                await repository.save_object_async(tweet)
                await aioify(session.commit)()
            except Exception as ex:
                print("Tweet persist failed", ex, tweet)
                failed_persist_tweets.append((tweet, ex))
                await aioify(session.rollback)()

        await twitterscraper.controllers.jobs.set_job_finalized(job.job_id)

    print(f"{len(tweets) - len(failed_persist_tweets)} tweets persisted, {len(failed_persist_tweets)} failed")

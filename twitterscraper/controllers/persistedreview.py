import json
import asyncio
from typing import *

from twitterscraper.services import AMQPClient, Repository, TwitterNitterClient
from twitterscraper.models import PersistedReviewJob, TwitterTweet
from twitterscraper.settings import MainSettings, AMQPSettings
from twitterscraper.utils import get_timestamp
import twitterscraper.controllers.jobs


async def run_persistedreview_worker(msg_limit=None):
    print("Run PersistedReview worker")
    amqp_settings: AMQPSettings = MainSettings.get().amqp
    queue_settings = amqp_settings.queues.persistedreview
    await AMQPClient.get().consume(
        queue=queue_settings.name,
        callback=_persistedreview_job_callback,
        workers=queue_settings.workers,
        msg_limit=msg_limit
    )


async def enqueue_persistedreview_jobs(*jobs: PersistedReviewJob):
    settings: AMQPSettings = MainSettings.get().amqp
    queue_settings = settings.queues.persistedreview
    await twitterscraper.controllers.jobs.enqueue_jobs(
        *jobs,
        exchange=settings.exchange,
        routingkey=queue_settings.name,
        persistent=queue_settings.persistent
    )


async def _persistedreview_job_callback(payload: bytes):
    payload_js = json.loads(payload)
    job = PersistedReviewJob(**payload_js)
    repository = Repository.get()
    async with repository.session_async():
        profile = await repository.get_profile_by(userid=job.userid)
        remaining_tweets, removed_tweets = await _get_tweets_differences(
            username=profile.username,
            from_ts=job.from_timestamp,
            to_ts=job.to_timestamp
        )
        await _update_tweets_timestamps(remaining_tweets, removed_tweets)

    await twitterscraper.controllers.jobs.set_job_finalized(job.job_id)


async def _get_tweets_differences(username: str, from_ts: int, to_ts: int) -> Tuple[List[TwitterTweet], List[TwitterTweet]]:
    """Compare the tweets from a user, during the given time range, between those persisted and those fetched now.
    Detect which tweets remain and which are deleted.
    Return tuple of List[TwitterTweet], where:

    - first value corresponds to tweets that already exist.
    - seconds value corresponds to tweets that NO LONGER exist.
    """
    # NOTE assuming from_ts is always 00:00h of a certain day
    persisted_tweets = await Repository.get().get_tweets(
        username=username,
        from_ts=from_ts,
        to_ts=to_ts,
        filter_active_tweets=True
    )
    if not persisted_tweets:
        return [], []

    # TODO Avoid getting COMPLETE tweets data from Nitter... or keep doing so to get possibly missing tweets
    online_tweets = await TwitterNitterClient.get().get_tweets_in_range(
        username=username,
        from_timestamp=from_ts,
        to_timestamp=to_ts,
        include_replies=True
    )

    persisted_tweets = _tweets_list_to_dict(persisted_tweets)
    online_tweets = _tweets_list_to_dict(online_tweets)
    persisted_tweets_ids = set(persisted_tweets)
    online_tweets_ids = set(online_tweets)

    removed_tweets_ids = persisted_tweets_ids - online_tweets_ids
    # Double-check by verifying each tweet individually - generic search may return less tweets
    # (for example, tweets quoting a tweet that no longer exists are not returned on search, although still exist)
    if removed_tweets_ids:
        print(f"Found {len(removed_tweets_ids)} removed tweets, double-checking individually... {removed_tweets_ids}")
        removed_tweets_ids = await TwitterNitterClient.get().get_tweets_removed(removed_tweets_ids)
        print(f"Finally detected {len(removed_tweets_ids)} removed tweets: {removed_tweets_ids}")

    remaining_tweets_ids = persisted_tweets_ids - removed_tweets_ids
    removed_tweets = [persisted_tweets[tweet_id] for tweet_id in removed_tweets_ids]
    remaining_tweets = [persisted_tweets[tweet_id] for tweet_id in remaining_tweets_ids]

    print(f"Tweets differences: persisted={len(persisted_tweets)} remaining={len(remaining_tweets)} removed={len(removed_tweets)}")
    if removed_tweets:
        print(f"Removed tweets: {removed_tweets}")

    return remaining_tweets, removed_tweets


async def _update_tweets_timestamps(remaining_tweets: List[TwitterTweet], removed_tweets: List[TwitterTweet]):
    repository = Repository.get()
    now = get_timestamp()

    coroutines = list()
    for remaining_tweet in remaining_tweets:
        remaining_tweet.last_review_timestamp = now
        coroutines.append(repository.save_object_async(remaining_tweet))
    for removed_tweet in removed_tweets:
        removed_tweet.last_review_timestamp = now
        removed_tweet.deletion_detected_timestamp = now
        coroutines.append(repository.save_object_async(removed_tweet))

    await asyncio.gather(*coroutines)


def _tweets_list_to_dict(tweets: List[TwitterTweet]) -> Dict[str, TwitterTweet]:
    return {tweet.tweet_id: tweet for tweet in tweets}

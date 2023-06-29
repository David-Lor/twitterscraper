import asyncio

import procrastinate

from . import models
from ..repository import Repository
from ..twitterclient import Twitterclient


def setup_tasks(app: procrastinate.App, repository: Repository, twitter: Twitterclient):

    # noinspection PyPep8Naming,DuplicatedCode
    @app.task(name=models.InitialScanProfileTweetsV1.__name__, retry=True)
    async def task_InitialScanProfileTweetsV1(data_dict: dict):
        data = models.InitialScanProfileTweetsV1.parse_obj(data_dict)
        print("Task:", data.__class__.__name__, "start", data_dict)

        user = await repository.get_profile_by_id(data.profile_id)
        date_from, date_to_exc = data.date_from, data.date_to_exclusive
        tweets = await twitter.get_user_tweets_list(
            username=user.username,
            date_from=date_from,
            date_to_exc=date_to_exc,
        )

        print(f"Found {len(tweets)} tweets between {data.date_from.isoformat()} ~ {data.date_to_inc.isoformat()}")
        await repository.write_tweets(userid=user.userid, tweets=tweets)

        print("Task:", data.__class__.__name__, "COMPLETED", data_dict)

    # noinspection PyPep8Naming,DuplicatedCode
    @app.task(name=models.ReScanProfileTweetsV1.__name__, retry=True)
    async def task_ReScanProfileTweetsV1(data_dict: dict):
        data = models.ReScanProfileTweetsV1.parse_obj(data_dict)
        print("Task:", data.__class__.__name__, "start", data_dict)

        user = await repository.get_profile_by_id(data.profile_id)
        date_from, date_to_exc = data.date_from, data.date_to_exclusive
        tweets_now, tweets_persisted = await asyncio.gather(
            twitter.get_user_tweets_list(
                username=user.username,
                date_from=date_from,
                date_to_exc=date_to_exc,
            ),
            repository.get_tweets_by_profile_and_daterange(
                userid=user.userid,
                date_from=date_from,
                date_to_exc=date_to_exc,
            )
        )

        tweets_ids_now = set(tweet.tweet_id for tweet in tweets_now)
        tweets_ids_persisted = set(tweet.tweetid for tweet in tweets_persisted)
        tweets_ids_detected_as_deleted = tweets_ids_persisted.difference(tweets_ids_now)
        tweets_ids_detected_as_new = tweets_ids_now.difference(tweets_ids_persisted)
        tweets_detected_as_new = [tweet for tweet in tweets_now if tweet.tweet_id in tweets_ids_detected_as_new]

        await asyncio.gather(*[
            repository.write_tweets(userid=user.userid, tweets=tweets_detected_as_new),
            repository.update_deleted_tweets(userid=user.userid, tweets_ids=tweets_ids_detected_as_deleted),
        ])

        print("Task:", data.__class__.__name__, "COMPLETED", data_dict)

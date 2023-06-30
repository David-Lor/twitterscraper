import asyncio
from collections.abc import Sequence

from . import models
from .jobmanager import Jobmanager
from ..repository import Repository
from ..twitterclient import Twitterclient
from ..archiveorg import Archiveorg


def setup_tasks(jobmanager: Jobmanager, repository: Repository, twitter: Twitterclient):
    app = jobmanager.app

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
        tweets_ids = [tweet.tweet_id for tweet in tweets]
        await asyncio.gather(
            repository.write_tweets(userid=user.userid, tweets=tweets),
            _schedule_new_tweets_archiveorg(userid=user.userid, username=user.username, tweets_ids=tweets_ids)
        )

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
        tweets_ids_detected_as_new = [tweet.tweet_id for tweet in tweets_detected_as_new]

        await asyncio.gather(
            repository.write_tweets(userid=user.userid, tweets=tweets_detected_as_new),
            _schedule_new_tweets_archiveorg(userid=user.userid, username=user.username, tweets_ids=tweets_ids_detected_as_new),
            repository.update_deleted_tweets(userid=user.userid, tweets_ids=tweets_ids_detected_as_deleted)
        )

        print("Task:", data.__class__.__name__, "COMPLETED", data_dict)

    # noinspection PyPep8Naming
    @app.task(name=models.ArchiveorgTweetV1.__name__, retry=True)
    async def task_ArchiveorgTweetV1(data_dict: dict):
        data = models.ArchiveorgTweetV1.parse_obj(data_dict)
        print("Task:", data.__class__.__name__, "start", data_dict)

        url = f"https://www.twitter.com/{data.username}/status/{data.tweet_id}"
        archived_url = await Archiveorg.archive_url_or_get_latest(url)

        await repository.update_tweet_archive_url(
            tweet_id=data.tweet_id,
            userid=data.userid,
            archive_url=archived_url,
        )

        print("Task:", data.__class__.__name__, "COMPLETED", data_dict, "ArchiveURL:", archived_url)

    async def _schedule_new_tweets_archiveorg(userid: int, username: str, tweets_ids: Sequence[int]):
        await asyncio.gather(
            jobmanager.create_tweets_archiveorg_tasks(userid=userid, username=username, tweets_ids=tweets_ids),
            repository.update_archive_scheduled_tweets(userid=userid, tweets_ids=tweets_ids)
        )

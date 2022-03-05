import asyncio
import datetime
from typing import *

import sqlmodel
from aioify import aioify

import twitterscraper.entrypoint
from twitterscraper.services import Repository
from twitterscraper.models import TwitterProfile, TwitterTweet
from twitterscraper.utils import get_uuid


class BaseTest:
    _id_prefix = "test_"

    @classmethod
    def setup_class(cls):
        asyncio.get_event_loop().run_until_complete(twitterscraper.entrypoint.setup())

    @classmethod
    def teardown_method(cls):
        asyncio.get_event_loop().run_until_complete(cls.clear_db())

    @classmethod
    async def clear_db(cls):
        print("Test Teardown")
        repository = Repository.get()
        async with repository.session_async() as session:
            all_profiles, all_tweets = await asyncio.gather(
                aioify(session.exec(sqlmodel.select(TwitterProfile)).all)(),
                aioify(session.exec(sqlmodel.select(TwitterTweet)).all)()
            )

            coroutines_delete = list()
            for tweet in all_tweets:
                if tweet.tweet_id.startswith(cls._id_prefix):
                    coroutines_delete.append(aioify(session.delete)(tweet))

            for profile in all_profiles:
                if profile.userid.startswith(cls._id_prefix):
                    coroutines_delete.append(aioify(session.delete)(profile))

            await asyncio.gather(*coroutines_delete)

        # with Repository.get().session() as session:
        #     queries = [
        #         sqlmodel.delete(TwitterProfile).where(TwitterProfile.userid.startswith(cls._id_prefix)),
        #         sqlmodel.delete(TwitterTweet).where(TwitterTweet.tweet_id.startswith(cls._id_prefix))
        #     ]
        #     for query in queries:
        #         session.exec(query)
        # # TODO How to do WHERE LIKE in sqlmodel? (maybe we can do sqlalchemy-like)

    @classmethod
    def get_profile(cls, **kwargs):
        profile = TwitterProfile(
            userid=cls._id_prefix + get_uuid(),
            username=get_uuid(),
            joined_date=datetime.date.today(),
            active=True
        )
        for k, v in kwargs.items():
            profile.__setattr__(k, v)
        return profile

    @classmethod
    def get_tweet(cls, profile: TwitterProfile, **kwargs):
        tweet = TwitterTweet(
            tweet_id=cls._id_prefix + get_uuid(),
            text=get_uuid(),
            timestamp=datetime.datetime.now().timestamp(),
            is_reply=False,
            profile=profile
        )
        for k, v in kwargs.items():
            tweet.__setattr__(k, v)
        return tweet

    @staticmethod
    def tweets_to_ids(tweets: List[TwitterTweet]) -> Set[str]:
        return {tweet.tweet_id for tweet in tweets}

    @staticmethod
    def tweets_to_dict(tweets: List[TwitterTweet]) -> Dict[str, TwitterTweet]:
        return {tweet.tweet_id: tweet for tweet in tweets}

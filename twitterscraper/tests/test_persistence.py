import asyncio
import random

import pytest
import sqlmodel

from twitterscraper.models import TwitterTweet
from twitterscraper.services import Repository
from twitterscraper.utils import get_timestamp

from .base import BaseTest


class TestPersistProfile(BaseTest):
    def test_save_and_get_profile(self):
        repository = Repository.get()
        with repository.session():
            profile = self.get_profile()
            repository.save_object(profile)

            read_profile = Repository.get().get_profile_by_userid(profile.userid)
            assert read_profile.dict() == profile.dict()


# noinspection DuplicatedCode
class TestPersistTweet(BaseTest):
    # TODO Adapt all tests to async
    def test_save_tweet(self):
        repository = Repository.get()
        with repository.session():
            profile = self.get_profile()
            repository.save_object(profile, flush=True)

            tweet = self.get_tweet(profile)
            repository.save_object(tweet)

    def test_save_tweet_repeated_commiting_rollingback_in_place(self):
        repository = Repository.get()
        with repository.session() as session:
            profile = self.get_profile()
            repository.save_object(profile, flush=True)
            session.commit()

            tweets = [self.get_tweet(profile) for _ in range(5)]
            tweets[2].tweet_id = tweets[0].tweet_id
            expected_saved_tweets_ids = {tweet.tweet_id for tweet in tweets}

            failed_inserts = 0
            for tweet in tweets:
                # noinspection PyBroadException
                try:
                    print("Saving tweet", tweet.tweet_id)
                    repository.save_object(tweet, flush=True)
                    print("Saved tweet", tweet.tweet_id)
                    session.commit()
                except Exception as ex:
                    print("Failed saving tweet", tweet.tweet_id, ex)
                    session.rollback()
                    failed_inserts += 1

            assert failed_inserts == 1

        with repository.session() as session:
            for tweet_id in expected_saved_tweets_ids:
                query = sqlmodel.select(TwitterTweet).where(TwitterTweet.tweet_id == tweet_id)
                results = session.exec(query).all()
                assert len(results) == 1

    @pytest.mark.asyncio
    async def test_tweets_iterator(self):
        repository = Repository.get()
        async with repository.session_async():
            profile = self.get_profile()
            repository.save_object(profile, flush=True)

            # tweets_count / batch_size = expected read batches count, must be exact divisible
            tweets_count = 50
            batch_size = 10
            tweets = [self.get_tweet(profile) for _ in range(tweets_count)]
            await asyncio.gather(*[repository.save_object_async(tweet) for tweet in tweets])

            read_batches_count = 0
            read_tweets = list()
            async for tweets_batch in repository.tweets_iterator(batch_size=batch_size, userid=profile.userid):
                read_batches_count += 1
                read_tweets.extend(tweets_batch)

            assert read_batches_count == tweets_count / batch_size
            assert len(read_tweets) == len(tweets)
            assert read_tweets == tweets

    @pytest.mark.asyncio
    async def test_tweets_iterator_filter_ids_list(self):
        repository = Repository.get()
        async with repository.session_async():
            profile = self.get_profile()
            repository.save_object(profile, flush=True)

            # tweets_count / batch_size = expected read batches count, must be exact divisible
            tweets_count = 100
            tweets_get_count = 50
            batch_size = 10
            tweets = [self.get_tweet(profile) for _ in range(tweets_count)]
            await asyncio.gather(*[repository.save_object_async(tweet) for tweet in tweets])
            tweets_get = random.sample(tweets, tweets_get_count)
            tweets_get_ids = {tweet.tweet_id for tweet in tweets_get}

            read_batches_count = 0
            read_tweets = list()
            async for tweets_batch in repository.tweets_iterator(batch_size=batch_size, tweets_ids=list(tweets_get_ids)):
                read_batches_count += 1
                read_tweets.extend(tweets_batch)

            assert read_batches_count == tweets_get_count / batch_size
            assert len(read_tweets) == tweets_get_count
            assert {tweet.tweet_id for tweet in read_tweets} == tweets_get_ids

    @pytest.mark.asyncio
    async def test_get_tweets_filter_active_tweets(self):
        repository = Repository.get()
        async with repository.session_async():
            profile = self.get_profile()
            repository.save_object(profile, flush=True)

            tweets_count = 5
            tweets_active_count = 3
            tweets_inactive_count = tweets_count - tweets_active_count
            deletion_timestamp = get_timestamp()
            tweets_active = [self.get_tweet(profile) for _ in range(tweets_active_count)]
            tweets_inactive = [self.get_tweet(profile, deletion_detected_timestamp=deletion_timestamp) for _ in range(tweets_inactive_count)]
            all_tweets = [*tweets_active, *tweets_inactive]
            await repository.save_object_async(*all_tweets)

            read_all_tweets, read_active_tweets, read_inactive_tweets = await asyncio.gather(
                repository.get_tweets(),
                repository.get_tweets(filter_active_tweets=True),
                repository.get_tweets(filter_active_tweets=False)
            )

            assert len(read_all_tweets) == tweets_count
            assert len(read_active_tweets) == tweets_active_count
            assert len(read_inactive_tweets) == tweets_inactive_count

            assert self.tweets_to_ids(read_all_tweets) == self.tweets_to_ids(all_tweets)
            assert self.tweets_to_ids(read_active_tweets) == self.tweets_to_ids(tweets_active)
            assert self.tweets_to_ids(read_inactive_tweets) == self.tweets_to_ids(tweets_inactive)

            assert self.tweets_to_dict(all_tweets) == self.tweets_to_dict(read_all_tweets)
            assert self.tweets_to_dict(tweets_active) == self.tweets_to_dict(read_active_tweets)
            assert self.tweets_to_dict(tweets_inactive) == self.tweets_to_dict(read_inactive_tweets)

    @pytest.mark.asyncio
    async def test_get_tweets_filter_active_users(self):
        repository = Repository.get()
        async with repository.session_async():
            active_profiles_count = 3
            inactive_profiles_count = 2
            tweets_count_per_profile = 3

            active_profiles = [self.get_profile() for _ in range(active_profiles_count)]
            inactive_profiles = [self.get_profile(active=False) for _ in range(inactive_profiles_count)]
            all_profiles = [*active_profiles, *inactive_profiles]
            await repository.save_object_async(*all_profiles, flush=True)

            active_profiles_tweets = list()
            inactive_profiles_tweets = list()
            for active, profiles in [(True, active_profiles), (False, inactive_profiles)]:
                for profile in profiles:
                    profile_tweets = [self.get_tweet(profile) for _ in range(tweets_count_per_profile)]
                    if active:
                        active_profiles_tweets.extend(profile_tweets)
                    else:
                        inactive_profiles_tweets.extend(profile_tweets)
            all_tweets = [*active_profiles_tweets, *inactive_profiles_tweets]
            await repository.save_object_async(*all_tweets, flush=True)

            read_all_tweets, read_active_tweets, read_inactive_tweets = await asyncio.gather(
                repository.get_tweets(),
                repository.get_tweets(filter_active_profiles=True),
                repository.get_tweets(filter_active_profiles=False),
            )

            assert len(read_all_tweets) == tweets_count_per_profile * (active_profiles_count + inactive_profiles_count)
            assert len(read_active_tweets) == tweets_count_per_profile * active_profiles_count
            assert len(read_inactive_tweets) == tweets_count_per_profile * inactive_profiles_count

            assert self.tweets_to_ids(read_all_tweets) == self.tweets_to_ids(all_tweets)
            assert self.tweets_to_ids(read_active_tweets) == self.tweets_to_ids(active_profiles_tweets)
            assert self.tweets_to_ids(read_inactive_tweets) == self.tweets_to_ids(inactive_profiles_tweets)

            assert self.tweets_to_dict(read_all_tweets) == self.tweets_to_dict(all_tweets)
            assert self.tweets_to_dict(read_active_tweets) == self.tweets_to_dict(active_profiles_tweets)
            assert self.tweets_to_dict(read_inactive_tweets) == self.tweets_to_dict(inactive_profiles_tweets)

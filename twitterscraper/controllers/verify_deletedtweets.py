import asyncio
from typing import *

from twitterscraper.models import TwitterTweet
from twitterscraper.services import Repository, TwitterNitterClient
from twitterscraper.settings import MainSettings


async def run_verify_deletedtweets():
    async with Repository.get().session_async():
        tweets = await Repository.get().get_tweets(filter_active_tweets=False)
        servers = MainSettings.get().twitter.nitter_baseurl
        coroutines = list()
        for server in servers:
            for tweet in tweets:
                coroutines.append(_verify_tweet_against_server(tweet, server))

        results: Iterable[Tuple[TwitterTweet, str, bool]] = await asyncio.gather(*coroutines)
        results = sorted(results, key=_sort_verify_tweet_against_server)
        for result in results:
            print(result[0].url, result[1], "EXIST" if result[2] else "NOT-Exist")


async def _verify_tweet_against_server(tweet: TwitterTweet, server: str) -> Tuple[TwitterTweet, str, bool]:
    print("Verifying", tweet.tweet_id)
    status = await TwitterNitterClient([server]).get_tweet_status(tweet.tweet_id)
    return tweet, server, status.exists


def _sort_verify_tweet_against_server(tpl: Tuple[TwitterTweet, str, bool]):
    tweet, server, exists = tpl
    return tweet.tweet_id, server, exists

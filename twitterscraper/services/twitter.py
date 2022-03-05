import datetime
import asyncio
import random
from urllib.parse import urljoin
from typing import *

import requests
import tweepy
from aioify import aioify
from parse import parse
from bs4 import BeautifulSoup

from twitterscraper.models.domain import TwitterProfile, TwitterTweet, TweetScanStatus
from twitterscraper.utils import (
    Singleton, datetime_to_timestamp, timestamp_to_datetime, datetime_to_twitter_isoformat, timestamp_in_range
)


class TwitterProfileNotFoundError(Exception):
    def __init__(self, userid=None, username=None):
        self.userid = userid
        self.username = username

    def __str__(self):
        identifier = ""
        if self.username:
            identifier = f"username={self.username}"
        elif self.userid:
            identifier = f"userid={self.userid}"
        return f"The profile {identifier} does not exists".replace("  ", " ")


class TwitterAPIClient(Singleton):
    get: Callable[..., "TwitterAPIClient"]
    _twitter: tweepy.Client
    _twitterapi_minimum_datetime = datetime.datetime.fromisoformat("2010-11-06T00:00:01+00:00")
    """Twitter API does not allow fetching tweets before this datetime"""

    def __init__(self, api_key: str, api_secret: str, api_token: str):
        self._twitter = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            bearer_token=api_token
        )

    async def get_userinfo(self, username: str) -> TwitterProfile:
        data = await aioify(self._twitter.get_user)(
            username=username,
            user_fields=["id", "created_at"]
        )
        data = data.data
        if not data:
            raise TwitterProfileNotFoundError(username=username)

        return TwitterProfile(
            username=username,
            userid=data.id,
            joined_date=data.created_at
        )

    async def get_username(self, userid: str) -> str:
        data = await aioify(self._twitter.get_user)(
            id=userid,
            user_fields=["username"]
        )
        data = data.data
        if not data:
            raise TwitterProfileNotFoundError(userid=userid)

        return data.username

    def get_tweets_status(self, tweets_ids: List[str]) -> Dict[str, TweetScanStatus]:
        # TODO Deprecate
        batch_limit = 100
        if len(tweets_ids) > batch_limit:
            raise Exception(f"No more than {batch_limit} tweets can be requested")

        found_tweets: Dict[str, TweetScanStatus] = dict()
        response = self._twitter.get_tweets(
            ids=tweets_ids,
            tweet_fields=["created_at"]
        )
        for tweet_found in response.data:
            tweet_found: tweepy.Tweet
            try:
                tweet_scanstatus = TweetScanStatus(
                    tweet_id=tweet_found.id,
                    exists=True,
                    timestamp=datetime_to_timestamp(tweet_found.created_at)
                )
                found_tweets[tweet_scanstatus.tweet_id] = tweet_scanstatus
            except Exception as ex:
                print("Error parsing tweet scan status", tweet_found, ex)

        notfound_tweets = {
            tweet_id: TweetScanStatus(
                tweet_id=tweet_id,
                exists=False
            )
            for tweet_id in tweets_ids
            if tweet_id not in found_tweets
        }
        return {**found_tweets, **notfound_tweets}

    async def get_tweets_status_async(self, tweets_ids: List[str]) -> Dict[str, TweetScanStatus]:
        # TODO Deprecate
        return await aioify(self.get_tweets_status)(tweets_ids)

    def get_tweets_in_range(
            self,
            userid: str,
            from_timestamp: int,
            to_timestamp: int,
            include_replies: bool = True,
            page_size: int = 100
    ) -> List[TwitterTweet]:
        # TODO Deprecate
        tweets = list()
        pagination_token = False
        exclude = ["retweets"]
        if include_replies:
            exclude.append("replies")

        while pagination_token is not None:
            _pagination_token = pagination_token if isinstance(pagination_token, str) else None
            start_time = timestamp_to_datetime(from_timestamp)
            end_time = timestamp_to_datetime(to_timestamp)
            if end_time < self._twitterapi_minimum_datetime:
                return tweets
            if start_time < self._twitterapi_minimum_datetime:
                start_time = self._twitterapi_minimum_datetime

            print(f"Requesting tweets batch (paginationToken={pagination_token})")
            response: tweepy.Response = self._twitter.get_users_tweets(
                id=userid,
                start_time=datetime_to_twitter_isoformat(start_time),
                end_time=datetime_to_twitter_isoformat(end_time),
                exclude=exclude,
                tweet_fields=["id", "created_at", "text", "conversation_id"],
                user_fields=[],
                max_results=page_size,
                pagination_token=_pagination_token,
            )

            pagination_token = response.meta.get("next_token")
            response_tweets = response.data or []

            print(f"Found {len(response_tweets)} tweets in batch")
            for response_tweet in response_tweets:
                response_tweet: tweepy.Tweet
                tweet_id = response_tweet.id
                tweet_text = response_tweet.text
                tweet_datetime = response_tweet.created_at
                tweet_is_reply = response_tweet.id != response_tweet.conversation_id

                tweets.append(TwitterTweet(
                    profile_id=userid,
                    tweet_id=tweet_id,
                    text=tweet_text,
                    timestamp=datetime_to_timestamp(tweet_datetime),
                    is_reply=tweet_is_reply
                ))

        print(f"Total found {len(tweets)} tweets")
        return tweets

    async def get_tweets_in_range_async(
            self,
            userid: str,
            from_timestamp: int,
            to_timestamp: int,
            include_replies: bool = True,
            page_size: int = 100
    ) -> List[TwitterTweet]:
        # TODO Deprecate
        return await aioify(self.get_tweets_in_range)(userid, from_timestamp, to_timestamp, include_replies, page_size)


class TwitterNitterClient(Singleton):
    get: Callable[..., "TwitterNitterClient"]
    _nitter_baseurls: List[str]

    def __init__(self, baseurls: List[str]):
        if not baseurls:
            raise Exception("No baseurls given for TwitterNitterClient")
        self._nitter_baseurls = baseurls
        self._nitter_baseurls_unique = set(baseurls)

    def pick_nitter_baseurl(self) -> str:
        return random.choice(self._nitter_baseurls)

    async def get_tweets_removed(self, tweets_ids: Iterable[str]) -> Set[str]:
        statuses = await self.get_tweets_status(tweets_ids=tweets_ids, ensure=True)
        return {status.tweet_id for status in statuses.values() if not status.exists}

    async def get_tweets_status(self, tweets_ids: Iterable[str], ensure: bool = False) -> Dict[str, TweetScanStatus]:
        statuses = await asyncio.gather(*[
            self.get_tweet_status_ensure(tweet_id) if ensure else self.get_tweet_status(tweet_id)
            for tweet_id in tweets_ids
        ])
        return {status.tweet_id: status for status in statuses}

    async def get_tweet_status_ensure(self, tweet_id: str) -> TweetScanStatus:
        """Call get_tweet_status for each one of the available servers, until one of them reports the tweet exists.
        This is done for fixing a problem with Nitter, randomly reporting that a tweet does not exist,
        when it actually exists. It may be a quota or regional issue."""
        for server in self._nitter_baseurls_unique:
            status = await self.get_tweet_status(tweet_id=tweet_id, server=server)
            if status.exists:
                break
        # noinspection PyUnboundLocalVariable
        return status

    async def get_tweet_status(self, tweet_id: str, server: Optional[str] = None) -> TweetScanStatus:
        if server is None:
            server = self.pick_nitter_baseurl()
        url_suffix = f"/status/status/{tweet_id}"
        url = urljoin(server, url_suffix)
        r: requests.Response = await aioify(requests.get)(url)

        if r.status_code == 404 and "Tweet not found" in r.text:
            exists = False
        else:
            r.raise_for_status()
            # TODO more validations?
            exists = True

        print(f"Tweet {tweet_id} status on {server}: {exists}")
        return TweetScanStatus(
            exists=exists,
            tweet_id=tweet_id
        )

    async def get_tweets_in_range(
            self,
            username: str,
            from_timestamp: int,
            to_timestamp: int,
            include_replies: bool = True
    ) -> List[TwitterTweet]:
        # from_timestamp inclusive, to_timestamp exclusive
        from_datetime = timestamp_to_datetime(from_timestamp)
        from_date = from_datetime.date()
        to_datetime = timestamp_to_datetime(to_timestamp)
        to_date = to_datetime.date()
        if (to_datetime.hour, to_datetime.minute, to_datetime.second) != (0, 0, 0):
            to_date += datetime.timedelta(days=1)  # compatibilize Nitter exclusive to-date filter

        from_date = from_date.isoformat()
        to_date = to_date.isoformat()

        nitter_baseurl = self.pick_nitter_baseurl()
        url_suffix = f"/{username}/search"
        next_urlparams = f"?f=tweets&q=&e-nativeretweets=on&since={from_date}&until={to_date}"
        if not include_replies:
            next_urlparams += "&e-replies=on"

        tweets = list()
        while next_urlparams is not None:
            url = urljoin(nitter_baseurl, url_suffix + next_urlparams)
            print("Requesting Nitter", url)
            r = await aioify(requests.get)(url)  # TODO use httpx
            r.raise_for_status()

            scroll_tweets, next_urlparams = self._nitter_parse_tweets(
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                body=r.text
            )
            print(f"{len(scroll_tweets)} found on Nitter scroll")
            tweets.extend(scroll_tweets)

        print(f"{len(tweets)} total tweets found on Nitter")
        return tweets

    @staticmethod
    def _nitter_parse_tweets(from_timestamp: int, to_timestamp: int, body: str) -> Tuple[List[TwitterTweet], Optional[str]]:
        """Parse tweets from a Nitter page body. Returns (found tweets, URL params to next page of results)
        URL params example:
        ?f=tweets&e-nativeretweets=on&since=2022-01-01&until=2022-02-22&cursor=scroll%3AthGAVUV0VFVBaSwL75487urSkWiMC54czMg74pEnEVyIV6FYCJehgHREVGQVVMVDUBFQAVAAA%3D
        """
        # TODO assert profile not found
        if "No more items" in body:
            return [], None

        tweets = list()
        html = BeautifulSoup(body, "html.parser")

        # timeline_item corresponds to a tweet div
        for timeline_item in html.find_all("div", class_="timeline-item"):
            # TODO log/identify when a tweet is not parseable (continue cmd)

            # tweet is reply
            tweet_is_reply = "Replying to" in timeline_item.text

            # tweet date
            html_tweet_date = timeline_item.find("span", class_="tweet-date")
            if not html_tweet_date:
                continue
            html_tweet_date_link = html_tweet_date.find("a")
            if not html_tweet_date_link:
                continue
            html_tweet_date_str = html_tweet_date_link.get("title")
            if not html_tweet_date_str:
                continue
            # date example: Feb 18, 2022 · 11:48 AM UTC
            # We can't get seconds :(
            tweet_date_format = "%b %d, %Y · %I:%M %p UTC"
            try:
                tweet_datetime = datetime.datetime.strptime(html_tweet_date_str, tweet_date_format). \
                    replace(tzinfo=datetime.timezone.utc)
                tweet_timestamp = datetime_to_timestamp(tweet_datetime)
            except ValueError:
                continue

            # exclude tweets out of time range
            if not timestamp_in_range(ts=tweet_timestamp, from_ts=from_timestamp, to_ts=to_timestamp):
                continue

            # tweet id
            html_tweet_link = timeline_item.find("a", class_="tweet-link")
            if not html_tweet_link:
                continue
            html_tweet_link_href: str = html_tweet_link.get("href")
            if not html_tweet_link_href:
                continue
            # TODO (import parse) parse.compile for having the same template once
            tweet_id_parse = parse("/{}/status/{tweet_id}#m", html_tweet_link_href)
            try:
                tweet_id = tweet_id_parse["tweet_id"]
            except KeyError:
                continue

            # tweet text
            html_tweet_content = timeline_item.find("div", class_="tweet-content")
            tweet_text = html_tweet_content.text
            if tweet_text is None:
                continue

            tweets.append(TwitterTweet(
                profile_id=None,
                tweet_id=tweet_id,
                text=tweet_text,
                timestamp=tweet_timestamp,
                is_reply=tweet_is_reply
            ))

        next_urlparams = None
        try:
            html_loadmore_link = next(a for a in html.find_all("a") if a.text == "Load more")
            next_urlparams = html_loadmore_link.get("href")
        except StopIteration:
            # TODO unexpected, always returns the button if current page has tweets
            pass

        return tweets, next_urlparams

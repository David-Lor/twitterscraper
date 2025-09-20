from .base import BaseTwitterScraper
from ..http import http_client
from ...models.twitter import Tweet
from ...settings import Settings
from ...utils import json, get_datetime_now, html_parse, parse_datetime


class SyndicationTwitterScraper(BaseTwitterScraper):

    def __init__(self):
        self.http_settings = Settings.get().scraper.http

    async def get_user_tweets(self, username: str) -> dict[int, Tweet] | None:
        with self.logger.contextualize(twitter_username=username):
            async with http_client(self.http_settings, with_twitter_cookies=True) as client:
                r = await client.get(f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{username}")

                if r.status_code == 404 or r.status_code == 429:
                    # 404 = may happen randomly
                    # 429 = too many requests
                    rate_limit_remaining = r.headers.get("x-rate-limit-remaining")
                    rate_limit_reset = r.headers.get("x-rate-limit-reset")
                    self.logger.bind(_statuscode=r.status_code, _ratelimit_remaining=rate_limit_remaining, _ratelimit_reset_on=rate_limit_reset)\
                        .warning("Rate-Limited")
                    return None

                r.raise_for_status()

                parser = html_parse(r.text)
                js_script = parser.find("script", attrs=dict(id="__NEXT_DATA__", type="application/json"))
                if not js_script:
                    raise Exception("Not found script containing json")

                # noinspection PyTypeChecker
                js = json.loads(js_script.contents[0])

                results = dict()
                for tweet in self._parse_tweets_from_syndication(js):
                    results[tweet.tweet_id] = tweet

                return results

    async def tweet_exists(self, tweet_id: int | str) -> bool | None:
        with self.logger.contextualize(tweet_id=tweet_id):
            async with http_client(self.http_settings, with_twitter_cookies=True) as client:
                r = await client.get(f"https://publish.twitter.com/oembed?url=https%3A%2F%2Ftwitter.com%2F___%2Fstatus%2F{tweet_id}")
                if r.status_code == 404:
                    # Tweet not exists, or may be partially blocked/censured. Random false positives?
                    self.logger.debug("Tweet may not exist (404)")
                    return False
                if r.status_code == 403:
                    # Account suspended
                    self.logger.debug("Account suspended (403)")
                    return None

                r.raise_for_status()
                return True

    @staticmethod
    def _parse_tweets_from_syndication(js: dict):
        for js_tweet in js["props"]["pageProps"]["timeline"]["entries"]:
            if not js_tweet["type"] == "tweet":
                continue

            js_tweet = js_tweet["content"]["tweet"]
            if js_tweet.get("retweeted_status"):
                continue

            yield Tweet(
                tweet_id=js_tweet["id_str"],
                text=js_tweet["full_text"],
                user=Tweet.User(
                    username=js_tweet["user"]["screen_name"],
                    id=js_tweet["user"]["id_str"],
                ),
                raw=js_tweet,
                when_published=parse_datetime(js_tweet["created_at"]),  # example: "Tue Nov 12 06:34:39 +0000 2024"
                when_scraped=get_datetime_now(),
            )

import datetime

import pnytter

from . import tables
from .. import utils


def pnytter_tweet_to_orm(userid: int, tweet: pnytter.TwitterTweet, scraped_on: datetime.datetime | None = None) -> tables.Tweet:
    if not scraped_on:
        scraped_on = datetime.datetime.utcnow()

    return tables.Tweet(
        tweetid=tweet.tweet_id,
        userid=userid,
        published_on=tweet.created_on.replace(tzinfo=None),
        data=dict(
            **utils.jsonable_encoder(tweet, exclude={"tweet_id", "created_on", "author"}),
            scraped_on=scraped_on.isoformat(),
        ),
    )

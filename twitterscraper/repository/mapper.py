import pnytter

from . import tables
from .. import utils


def pnytter_tweet_to_orm(userid: int, tweet: pnytter.TwitterTweet) -> tables.Tweet:
    return tables.Tweet(
        tweetid=tweet.tweet_id,
        userid=userid,
        published_on=tweet.created_on.replace(tzinfo=None),
        data=utils.jsonable_encoder(tweet, exclude={"tweet_id", "created_on", "author"})
    )

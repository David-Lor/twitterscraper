import datetime
import pydantic


class Tweet(pydantic.BaseModel):

    class User(pydantic.BaseModel):
        username: str
        id: int

    tweet_id: int
    text: str
    user: User
    raw: dict
    when_published: datetime.datetime
    when_scraped: datetime.datetime

    @property
    def url(self):
        return f"https://www.twitter.com/{self.user.username}/status/{self.tweet_id}"

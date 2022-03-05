import datetime

import datetimerange
import pytest

from twitterscraper.services import TwitterAPIClient, TwitterNitterClient
from twitterscraper.models import TwitterTweet
from twitterscraper.settings import load_settings
from twitterscraper.utils import datetime_to_timestamp, timestamp_to_datetime
from .base import BaseTest


class BaseTwitterTest(BaseTest):
    client_twitterapi: TwitterAPIClient
    client_nitter: TwitterNitterClient

    @classmethod
    def setup_class(cls):
        super().setup_class()
        settings = load_settings().twitter
        cls.client_twitterapi = TwitterAPIClient(
            api_key=settings.keys.key,
            api_secret=settings.keys.secret,
            api_token=settings.keys.token,
        )
        cls.client_nitter = TwitterNitterClient(settings.nitter_baseurl)


class TestNitter(BaseTwitterTest):
    @pytest.mark.asyncio
    async def test_get_tweets_in_range_possumeveryhour(self):
        userid = "123456"
        username = "possumeveryhour"
        from_datetime = datetime.datetime.fromisoformat("2022-01-01T00:00:00+00:00")
        to_datetime = datetime.datetime.fromisoformat(
            "2022-01-03T14:59:59+00:00")  # datetimerange 'to' is inclusive, but we want exclusive
        expected_last_datetime = datetime.datetime.fromisoformat("2022-01-03T14:00:00+00:00")
        expected_tweets_datetimes = list(
            datetimerange.DateTimeRange(from_datetime, to_datetime).range(datetime.timedelta(hours=1)))
        expected_tweets = [
            TwitterTweet(tweet_id='1477067061964775424', text='', timestamp=1640995200, is_reply=False),
            TwitterTweet(tweet_id='1477082160490242053', text='', timestamp=1640998800, is_reply=False),
            TwitterTweet(tweet_id='1477097253261217796', text='', timestamp=1641002400, is_reply=False),
            TwitterTweet(tweet_id='1477112354328489988', text='', timestamp=1641006000, is_reply=False),
            TwitterTweet(tweet_id='1477127454141652996', text='', timestamp=1641009600, is_reply=False),
            TwitterTweet(tweet_id='1477142551849320449', text='', timestamp=1641013200, is_reply=False),
            TwitterTweet(tweet_id='1477157652945936397', text='', timestamp=1641016800, is_reply=False),
            TwitterTweet(tweet_id='1477172751299493888', text='', timestamp=1641020400, is_reply=False),
            TwitterTweet(tweet_id='1477187850777108481', text='', timestamp=1641024000, is_reply=False),
            TwitterTweet(tweet_id='1477202950120517637', text='', timestamp=1641027600, is_reply=False),
            TwitterTweet(tweet_id='1477218051787530244', text='', timestamp=1641031200, is_reply=False),
            TwitterTweet(tweet_id='1477233147817381891', text='', timestamp=1641034800, is_reply=False),
            TwitterTweet(tweet_id='1477248250797305860', text='', timestamp=1641038400, is_reply=False),
            TwitterTweet(tweet_id='1477263348748230659', text='', timestamp=1641042000, is_reply=False),
            TwitterTweet(tweet_id='1477278448691425281', text='', timestamp=1641045600, is_reply=False),
            TwitterTweet(tweet_id='1477293548722692097', text='', timestamp=1641049200, is_reply=False),
            TwitterTweet(tweet_id='1477308646719639553', text='', timestamp=1641052800, is_reply=False),
            TwitterTweet(tweet_id='1477323746310594568', text='', timestamp=1641056400, is_reply=False),
            TwitterTweet(tweet_id='1477338848011116547', text='', timestamp=1641060000, is_reply=False),
            TwitterTweet(tweet_id='1477353947820089345', text='', timestamp=1641063600, is_reply=False),
            TwitterTweet(tweet_id='1477369046018445318', text='', timestamp=1641067200, is_reply=False),
            TwitterTweet(tweet_id='1477384142929240069', text='', timestamp=1641070800, is_reply=False),
            TwitterTweet(tweet_id='1477399249860112386', text='', timestamp=1641074400, is_reply=False),
            TwitterTweet(tweet_id='1477414344195383296', text='', timestamp=1641078000, is_reply=False),
            TwitterTweet(tweet_id='1477429444126121985', text='', timestamp=1641081600, is_reply=False),
            TwitterTweet(tweet_id='1477444541120720898', text='', timestamp=1641085200, is_reply=False),
            TwitterTweet(tweet_id='1477459644067065856', text='', timestamp=1641088800, is_reply=False),
            TwitterTweet(tweet_id='1477474743129452544', text='', timestamp=1641092400, is_reply=False),
            TwitterTweet(tweet_id='1477489842829279233', text='', timestamp=1641096000, is_reply=False),
            TwitterTweet(tweet_id='1477504940163670020', text='', timestamp=1641099600, is_reply=False),
            TwitterTweet(tweet_id='1477520038349479937', text='', timestamp=1641103200, is_reply=False),
            TwitterTweet(tweet_id='1477535140813459463', text='', timestamp=1641106800, is_reply=False),
            TwitterTweet(tweet_id='1477550240848879617', text='', timestamp=1641110400, is_reply=False),
            TwitterTweet(tweet_id='1477565336883011588', text='', timestamp=1641114000, is_reply=False),
            TwitterTweet(tweet_id='1477580436813623297', text='', timestamp=1641117600, is_reply=False),
            TwitterTweet(tweet_id='1477595539059445762', text='', timestamp=1641121200, is_reply=False),
            TwitterTweet(tweet_id='1477610638113492994', text='', timestamp=1641124800, is_reply=False),
            TwitterTweet(tweet_id='1477625735468699652', text='', timestamp=1641128400, is_reply=False),
            TwitterTweet(tweet_id='1477640835374239749', text='', timestamp=1641132000, is_reply=False),
            TwitterTweet(tweet_id='1477655933237006341', text='', timestamp=1641135600, is_reply=False),
            TwitterTweet(tweet_id='1477671037374509056', text='', timestamp=1641139200, is_reply=False),
            TwitterTweet(tweet_id='1477686134981505030', text='', timestamp=1641142800, is_reply=False),
            TwitterTweet(tweet_id='1477701237483130884', text='', timestamp=1641146400, is_reply=False),
            TwitterTweet(tweet_id='1477716334997876738', text='', timestamp=1641150000, is_reply=False),
            TwitterTweet(tweet_id='1477731431979835394', text='', timestamp=1641153600, is_reply=False),
            TwitterTweet(tweet_id='1477746533244231691', text='', timestamp=1641157200, is_reply=False),
            TwitterTweet(tweet_id='1477761633887965188', text='', timestamp=1641160800, is_reply=False),
            TwitterTweet(tweet_id='1477776731129929733', text='', timestamp=1641164400, is_reply=False),
            TwitterTweet(tweet_id='1477791836190216193', text='', timestamp=1641168000, is_reply=False),
            TwitterTweet(tweet_id='1477806929418276871', text='', timestamp=1641171600, is_reply=False),
            TwitterTweet(tweet_id='1477822031324499974', text='', timestamp=1641175200, is_reply=False),
            TwitterTweet(tweet_id='1477837129585725443', text='', timestamp=1641178800, is_reply=False),
            TwitterTweet(tweet_id='1477852235027685376', text='', timestamp=1641182400, is_reply=False),
            TwitterTweet(tweet_id='1477867334308188167', text='', timestamp=1641186000, is_reply=False),
            TwitterTweet(tweet_id='1477882431927693316', text='', timestamp=1641189600, is_reply=False),
            TwitterTweet(tweet_id='1477897532105711621', text='', timestamp=1641193200, is_reply=False),
            TwitterTweet(tweet_id='1477912630174097414', text='', timestamp=1641196800, is_reply=False),
            TwitterTweet(tweet_id='1477927728561197058', text='', timestamp=1641200400, is_reply=False),
            TwitterTweet(tweet_id='1477942825207615488', text='', timestamp=1641204000, is_reply=False),
            TwitterTweet(tweet_id='1477957927331713025', text='', timestamp=1641207600, is_reply=False),
            TwitterTweet(tweet_id='1477973024452362243', text='', timestamp=1641211200, is_reply=False),
            TwitterTweet(tweet_id='1477988124374573063', text='', timestamp=1641214800, is_reply=False),
            TwitterTweet(tweet_id='1478003225479548939', text='', timestamp=1641218400, is_reply=False)
        ]

        tweets = await self.client_nitter.get_tweets_in_range(
            username=username,
            from_timestamp=datetime_to_timestamp(from_datetime),
            to_timestamp=datetime_to_timestamp(to_datetime),
            include_replies=True
        )
        tweets = sorted(tweets, key=lambda tweet: tweet.timestamp)

        tweets_datetimes = [timestamp_to_datetime(tweet.timestamp) for tweet in tweets]
        assert tweets_datetimes == expected_tweets_datetimes
        assert tweets_datetimes[-1] == expected_last_datetime
        assert tweets == expected_tweets

import datetime

import procrastinate
import pnytter

from . import models
from ..repository import Repository


def process_tasks(app: procrastinate.App, repository: Repository, nitter: pnytter.Pnytter):

    # noinspection PyPep8Naming
    @app.task(name=models.ScanProfileTweetsV1.__name__, retry=True)
    async def task_ScanProfileTweetsV1(data_dict: dict):
        data = models.ScanProfileTweetsV1.parse_obj(data_dict)
        print("Task:", data.__class__.__name__, "start", data_dict)

        user = await repository.get_user_by_id(data.profile_id)
        tweets = nitter.get_user_tweets_list(
            username=user.username,
            filter_from=data.date_from,
            filter_to=data.date_to + datetime.timedelta(days=1),  # convert date_to inclusive to exclusive
        )

        if tweets:
            print(f"Found {len(tweets)} tweets between {data.date_from.isoformat()} ~ {data.date_to.isoformat()}")
            await repository.write_tweets(userid=user.userid, tweets=tweets)

        print("Task:", data.__class__.__name__, "COMPLETED", data_dict)

import datetime

import pydantic


class BaseTask(pydantic.BaseModel):
    """Tasks models names are used to name the tasks themselves.
    """
    pass


class InitialScanProfileTweetsV1(BaseTask):
    profile_id: int
    date_from: datetime.date
    date_to_inc: datetime.date  # inclusive

    @property
    def date_to_exclusive(self):
        return self.date_to_inc + datetime.timedelta(days=1)


class ReScanProfileTweetsV1(InitialScanProfileTweetsV1):
    pass


class ArchiveorgTweetV1(BaseTask):
    userid: int
    username: str
    tweet_id: int


TASKS_CLASSES = [InitialScanProfileTweetsV1, ReScanProfileTweetsV1, ArchiveorgTweetV1]

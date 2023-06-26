import datetime

import pydantic


class BaseTask(pydantic.BaseModel):
    """Tasks models names are used to name the tasks themselves.
    """
    pass


class ScanProfileTweetsV1(BaseTask):
    profile_id: int
    date_from: datetime.date
    date_to: datetime.date  # inclusive

    @property
    def date_to_exclusive(self):
        return self.date_to + datetime.timedelta(days=1)


TASKS_CLASSES = [ScanProfileTweetsV1]

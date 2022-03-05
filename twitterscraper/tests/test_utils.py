import datetime
from typing import *

import pytest
import pydantic

from twitterscraper.utils import date_to_datetime_range


class DateToDatetimeRangeScenario(pydantic.BaseModel):
    start_date: datetime.date
    end_datetime: datetime.datetime
    expected_intervals: List[Tuple[datetime.datetime, datetime.datetime]]


# noinspection PyTypeChecker
@pytest.mark.parametrize("scenario", [
    DateToDatetimeRangeScenario(
        start_date="2020-01-01",
        end_datetime="2020-01-02T00:00:00+00:00",
        expected_intervals=[
            ("2020-01-01T00:00:00+00:00", "2020-01-02T00:00:00+00:00")
        ]
    ),
    DateToDatetimeRangeScenario(
        start_date="2020-01-01",
        end_datetime="2020-01-01T00:00:00+00:00",
        expected_intervals=[]
    ),
    DateToDatetimeRangeScenario(
        start_date="2019-12-30",
        end_datetime="2020-01-02T00:00:00+00:00",
        expected_intervals=[
            ("2019-12-30T00:00:00+00:00", "2019-12-31T00:00:00+00:00"),
            ("2019-12-31T00:00:00+00:00", "2020-01-01T00:00:00+00:00"),
            ("2020-01-01T00:00:00+00:00", "2020-01-02T00:00:00+00:00"),
        ]
    ),
    DateToDatetimeRangeScenario(
        start_date="2020-01-01",
        end_datetime="2020-01-03T11:12:13+00:00",
        expected_intervals=[
            ("2020-01-01T00:00:00+00:00", "2020-01-02T00:00:00+00:00"),
            ("2020-01-02T00:00:00+00:00", "2020-01-03T00:00:00+00:00"),
            ("2020-01-03T00:00:00+00:00", "2020-01-03T11:12:13+00:00"),
        ]
    ),
    DateToDatetimeRangeScenario(
        start_date="2020-01-01",
        end_datetime="2020-01-02T00:00:01+00:00",
        expected_intervals=[
            ("2020-01-01T00:00:00+00:00", "2020-01-02T00:00:00+00:00"),
            ("2020-01-02T00:00:00+00:00", "2020-01-02T00:00:01+00:00"),
        ]
    ),
])
def test_date_to_datetime_range(scenario: DateToDatetimeRangeScenario):
    intervals = list(date_to_datetime_range(scenario.start_date, scenario.end_datetime))
    assert intervals == scenario.expected_intervals

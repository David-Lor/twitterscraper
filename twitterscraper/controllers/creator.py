import asyncio

from twitterscraper.services import Repository, TwitterAPIClient
from twitterscraper.models import TwitterProfile, FetchPersistJob
from twitterscraper.utils import daterange, day_to_timestamps, get_uuid
import twitterscraper.controllers.fetchandpersist


async def run_creator(username: str):
    profile = await TwitterAPIClient.get().get_userinfo(username)
    print("Found profile data", profile)

    async with Repository.get().session_async():
        await Repository.get().save_object_async(profile)
        await _create_profile_initial_fetchandpersist_jobs(profile)


async def _create_profile_initial_fetchandpersist_jobs(profile: TwitterProfile):
    to_ts = None
    jobs = list()

    coroutines = list()
    # one job per day, since (date when profile joined) until (yesterday inclusive)
    for day in daterange(start_date=profile.joined_date, end_inclusive=False):
        from_ts, to_ts = day_to_timestamps(day)
        jobs.append(FetchPersistJob(
            job_id=get_uuid(),
            userid=profile.userid,
            from_timestamp=from_ts,
            to_timestamp=to_ts
        ))

    if jobs:
        coroutines.append(twitterscraper.controllers.fetchandpersist.enqueue_fetchandpersist_jobs(*jobs))
    if to_ts:
        coroutines.append(twitterscraper.controllers.fetchandpersist.save_profile_last_scan_timestamp(profile.userid, to_ts))

    await asyncio.gather(*coroutines)

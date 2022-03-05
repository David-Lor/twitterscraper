import asyncio
import datetime
from typing import *

from twitterscraper.services import Repository, TwitterAPIClient, TwitterProfileNotFoundError
from twitterscraper.models import TwitterProfile, FetchPersistJob, PersistedReviewJob
from twitterscraper.utils import (
    get_timestamp, get_uuid, date_to_datetime_range, timestamp_to_datetime, datetime_to_timestamp
)
import twitterscraper.controllers.fetchandpersist
import twitterscraper.controllers.persistedreview


async def run_task_syncprofilestweets():
    print("Running task SyncProfilesTweets")

    repository = Repository.get()
    async with repository.session_async():
        # TODO also sync in reverse (inactive profiles that change to active)
        profiles = await repository.list_profiles_async(filter_active_profiles=True)
        print(f"{len(profiles)} active profiles in DB")

        profiles = await _sync_profiles_active(profiles)
        print(f"{len(profiles)} active profiles for SyncProfilesTweets")

        now_timestamp = get_timestamp()
        await asyncio.gather(*[
            _create_syncprofilestweets_profile_jobs(
                userid=profile.userid,
                from_date=profile.joined_date,
                to_ts=now_timestamp
            )
            for profile in profiles
        ])

    print("Task SyncProfilesTweets completed")


async def run_task_newtweetsscan():
    print("Running task NewTweetsScan")
    repository = Repository.get()
    async with repository.session_async():
        profiles = await repository.list_profiles_async(filter_active_profiles=True)
        coroutines = [_profile_new_tweets_scan(profile) for profile in profiles if profile.last_scan_timestamp]
        await asyncio.gather(*coroutines)

    print("Task NewTweetsScan completed")


async def _profile_new_tweets_scan(profile: TwitterProfile):
    repository = Repository.get()
    now = get_timestamp()
    # TODO create split tasks by time (one per day)
    job = FetchPersistJob(
        job_id=get_uuid(),
        userid=profile.userid,
        from_timestamp=profile.last_scan_timestamp,
        to_timestamp=now
    )
    await twitterscraper.controllers.fetchandpersist.enqueue_fetchandpersist_jobs(job)

    profile.last_scan_timestamp = now
    await repository.save_object_async(profile)


async def _sync_profiles_active(profiles: List[TwitterProfile]) -> List[TwitterProfile]:
    """Verify which profiles are still active. Active profiles are returned, while unactive profiles are ignored.
    The state of the inactive-detected profiles is persisted on DB. Usernames are synced with the DB."""
    results = await asyncio.gather(*[_sync_profile_active(profile) for profile in profiles])
    active_userids = [userid for userid, active in results if active]
    return [profile for profile in profiles if profile.userid in active_userids]


async def _sync_profile_active(profile: TwitterProfile) -> Tuple[str, bool]:
    """Verify if single TwitterProfile is still active. If active, return (userid, True).
    If inactive, persist the state in DB and return (userid, False). Username is synced with the DB."""
    try:
        current_username = await TwitterAPIClient.get().get_username(profile.userid)
        profile.username = current_username
        active = profile.active = True
    except TwitterProfileNotFoundError:
        # TODO what happens with private users? (still exist, but tweets can't be fetched) TEST WITH MY TEST USER
        active = profile.active = False
        print(f"Profile", profile, "changed to INACTIVE")

    await Repository.get().save_object_async(profile)
    return profile.userid, active


async def _create_syncprofilestweets_profile_jobs(userid: str, from_date: datetime.date, to_ts: int):
    """Create PersistedReview jobs for a single user, during the given timestamps range (inclusive-exclusive)."""
    # TODO Should generate jobs only for dates when the user has tweets (check from persisted)
    jobs = list()
    for from_datetime, to_datetime in date_to_datetime_range(from_date, timestamp_to_datetime(to_ts)):
        jobs.append(PersistedReviewJob(
            job_id=get_uuid(),
            userid=userid,
            from_timestamp=datetime_to_timestamp(from_datetime),
            to_timestamp=datetime_to_timestamp(to_datetime)
        ))

    await twitterscraper.controllers.persistedreview.enqueue_persistedreview_jobs(*jobs)

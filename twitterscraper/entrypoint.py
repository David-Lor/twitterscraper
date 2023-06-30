import sys
import abc
import asyncio
import atexit
import argparse
import datetime
from typing import List, Type, Optional

from .jobmanager import tasks
from .settings import load_settings
from .jobmanager import Jobmanager
from .repository import Repository
from .twitterclient import Twitterclient
from .common import Service, Runnable

commands_classes = dict()


def command(name: str):
    def wrapper(cls):
        commands_classes[name] = cls
        return cls
    return wrapper


class BaseApp(Runnable, abc.ABC):
    def __init__(self):
        self.services: List[Service] = list()
        self.settings = load_settings()

        self.jobmanager = Jobmanager(settings=self.settings.jobmanager)
        self.services.append(self.jobmanager)

        self.repository = Repository(settings=self.settings.repository)
        self.services.append(self.repository)

        self.twitterclient = Twitterclient(settings=self.settings.nitter)
        self.services.append(self.twitterclient)

        tasks.setup_tasks(
            jobmanager=self.jobmanager,
            repository=self.repository,
            twitter=self.twitterclient,
        )

    async def setup(self):
        await asyncio.gather(*[service.setup() for service in self.services])

        @atexit.register
        def __teardown_fn():
            asyncio.get_event_loop().run_until_complete(
                asyncio.gather(*[service.teardown() for service in self.services])
            )

    async def teardown(self):
        await asyncio.gather(*[service.teardown() for service in self.services])


@command("worker")
class CmdWorker(BaseApp):
    def __init__(self):
        super().__init__()
        parser = argparse.ArgumentParser(
            prog="Run task workers",
            description="",
            epilog="",
        )
        parser.add_argument("-w", "--workers", required=False, default=None)
        args, _ = parser.parse_known_args()

        if args.workers is not None:
            self.workers_amount = int(args.workers)
        else:
            self.workers_amount = self.settings.jobmanager.workers

    async def run(self):
        print("Running", self.workers_amount, "workers")
        await self.jobmanager.app.run_worker_async(
            concurrency=self.workers_amount,
        )


@command("add-profile")
class CmdAddProfile(BaseApp):
    def __init__(self):
        super().__init__()
        parser = argparse.ArgumentParser(
            prog='Add Profile',
            description='Add a new profile and schedule its initial scan.',
            epilog='Text at the bottom of help'
        )
        parser.add_argument("-u", "--username", required=True)
        args, _ = parser.parse_known_args()

        self.username = args.username

    async def run(self):
        profile = await self.twitterclient.find_user(username=self.username)
        if not profile:
            print(f"Profile @{self.username} not found!")
            return

        await self.repository.write_profile(profile=profile)
        print(f"Profile @{self.username} persisted!")

        await self.jobmanager.create_profile_initial_scan_tasks(profile)
        await self.repository.update_profile_last_scan(
            userid=profile.id,
            date=datetime.date.today(),
        )
        print(f"Profile @{self.username} initial scan tasks created!")


@command("rescan")
class CmdRescan(BaseApp):
    def __init__(self):
        super().__init__()
        parser = argparse.ArgumentParser(
            prog='Re-Scan',
            description='Schedule a re-scan of all or some of the profiles.',
            epilog='Text at the bottom of help'
        )
        parser.add_argument("-u", "--username", action="append", required=False)  # case insensitive
        parser.add_argument("-a", "--since-beginning", help="", action=argparse.BooleanOptionalAction)
        args, _ = parser.parse_known_args()

        self.usernames: list[str] | None = args.username
        self.since_beginning: bool = args.since_beginning

    async def run(self):
        profiles = await self.repository.get_profiles(filter_by_username=self.usernames)
        print(f"{len(profiles)} profiles found:", {prof.userid: prof.username for prof in profiles})

        today = datetime.date.today()
        await asyncio.gather(*[
            self.jobmanager.create_profile_rescan_tasks(
                userid=profile.userid,
                date_from=profile.joined_date if self.since_beginning else profile.lastscan_date,
                date_to_inc=today,
            )
            for profile in profiles
        ])

        await asyncio.gather(*[
            self.repository.update_profile_last_scan(
                userid=profile.userid,
                date=today
            )
            for profile in profiles
        ])


@command("enqueue-archiveorg")
class CmdEnqueueArchiveorg(BaseApp):
    def __init__(self):
        super().__init__()
        parser = argparse.ArgumentParser(
            prog="Enqueue Archive.org",
            description="Enqueue Archive.org tweets for all the tweets not yet enqueued",
            epilog=""
        )
        parser.add_argument("-u", "--username", action="append", required=False)  # case insensitive
        args, _ = parser.parse_known_args()

        self.usernames: list[str] | None = args.username

    async def run(self):
        profiles = await self.repository.get_profiles(filter_by_username=self.usernames)
        profiles = [profile for profile in profiles if profile.archiveorg_enabled]
        print("Profiles to run Archive.org on unscheduled tweets:", {prof.userid: prof.username for prof in profiles})

        for profile in profiles:
            for tweets_batch in await self.repository.get_tweets_not_archiveorg_enqueued(profile.userid, batch_size=100):
                tweets_ids_batch = [tweet.tweetid for tweet in tweets_batch]
                await self.jobmanager.create_tweets_archiveorg_tasks(
                    userid=profile.userid,
                    username=profile.username,
                    tweets_ids=tweets_ids_batch,
                )
                await self.repository.update_archive_scheduled_tweets(
                    userid=profile.userid,
                    tweets_ids=tweets_ids_batch,
                )


async def amain(cmd: Type[BaseApp]):
    cmd_inst = cmd()

    print("Setup", cmd.__name__)
    await cmd_inst.setup()

    print("Run", cmd.__name__)
    await cmd_inst.run()


def main():
    cmd: Optional[Type[BaseApp]] = None
    for arg in sys.argv:
        try:
            cmd = commands_classes[arg]
            break
        except KeyError:
            continue

    if not cmd:
        print(f"Invalid or not given entrypoint, must be one of: {list(commands_classes.keys())}")
        sys.exit(1)

    asyncio.get_event_loop().run_until_complete(amain(cmd))

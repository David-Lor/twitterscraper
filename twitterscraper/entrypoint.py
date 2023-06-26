import sys
import abc
import asyncio
import atexit
import argparse
from typing import List, Type, Optional

import pnytter

from .jobmanager import tasks
from .settings import load_settings
from .jobmanager import Jobmanager
from .repository import Repository
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

        self.pnytter = pnytter.Pnytter(
            nitter_instances=self.settings.nitter.instances_str,
            request_timeout=self.settings.nitter.request_timeout_seconds,
            beautifulsoup_parser="lxml",
        )

        tasks.process_tasks(
            app=self.jobmanager.app,
            repository=self.repository,
            nitter=self.pnytter,
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
    async def run(self):
        await self.jobmanager.app.run_worker_async()


@command("add-profile")
class CmdAddProfile(BaseApp):
    def __init__(self):
        super().__init__()
        parser = argparse.ArgumentParser(
            prog='ProgramName',
            description='What the program does',
            epilog='Text at the bottom of help'
        )
        parser.add_argument("-u", "--username", required=True)
        args, _ = parser.parse_known_args()

        self.username = args.username

    async def run(self):
        # TODO wrap on async: await loop.run_in_executor(...)
        profile = self.pnytter.find_user(username=self.username)
        if not profile:
            print(f"Profile @{self.username} not found!")

        await self.repository.write_profile(profile=profile)
        print(f"Profile @{self.username} persisted!")

        await self.jobmanager.create_profile_initial_scan_tasks(profile)
        print(f"Profile @{self.username} initial scan tasks created!")


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

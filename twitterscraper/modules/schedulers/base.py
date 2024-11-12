import abc
import asyncio
from ...settings import Schedule
from ...utils import sleep_event


class BaseScheduler(abc.ABC):
    def __init__(self, schedule: Schedule):
        self.schedule = schedule
        self.stop_event = asyncio.Event()

    async def run(self):
        if not self.schedule.enabled:
            print(self.scheduler_name, "is DISABLED")
            await self.stop_event.wait()
            return

        if self.schedule.initial:
            delay = self.schedule.initial.get_delay_with_jitter()
            print(self.scheduler_name, "Initial Wait for", delay, "s")
            await sleep_event(self.stop_event, delay)

        while not self.stop_event.is_set():
            try:
                await self.run_loop()
            except Exception as ex:
                print("Error", self.scheduler_name, ":", ex.__class__.__name__, ex)

            delay = self.schedule.get_delay_with_jitter()
            print(self.scheduler_name, "Wait for", delay, "s")
            await sleep_event(self.stop_event, delay)

    @abc.abstractmethod
    async def run_loop(self):
        pass

    @property
    def scheduler_name(self):
        return self.__class__.__name__

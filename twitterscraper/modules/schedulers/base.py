import abc
import asyncio
import time
from ...settings import Schedule
from ...utils import sleep_event, get_id
from ...logger import logger


class BaseScheduler(abc.ABC):
    def __init__(self, schedule: Schedule):
        self.schedule = schedule
        self.stop_event = asyncio.Event()

    async def run(self):
        with logger.contextualize(scheduler=self.scheduler_name):
            if not self.schedule.enabled:
                logger.info("Scheduler disabled")
                await self.stop_event.wait()
                return

            logger.info("Scheduler enabled")

            initial_delay = self.schedule.get_initial_delay()
            if initial_delay > 0:
                logger.bind(delay_seconds=initial_delay).debug(f"Initial wait")
                await sleep_event(self.stop_event, initial_delay)

            iteration = 0
            while not self.stop_event.is_set():
                iteration += 1
                iteration_id = get_id()
                start = time.time()

                with logger.contextualize(iteration_count=iteration, iteration_id=iteration_id):
                    # noinspection PyBroadException
                    try:
                        logger.debug("Running interation")
                        await self.run_loop()

                        elapsed = time.time() - start
                        logger.bind(elapsed_seconds=elapsed).debug("Completed iteration")

                    except Exception:
                        logger.exception("Iteration failed")

                    delay = self.schedule.get_delay_with_jitter()
                    logger.bind(delay_seconds=delay).debug("Waiting for next iteration")
                    await sleep_event(self.stop_event, delay)

    @abc.abstractmethod
    async def run_loop(self):
        pass

    @property
    def scheduler_name(self):
        return self.__class__.__name__

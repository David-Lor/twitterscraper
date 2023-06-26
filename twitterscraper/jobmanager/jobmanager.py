import datetime

import procrastinate
import pnytter

from . import models
from .. import utils
from ..common import Service
from ..settings import JobmanagerSettings


class Jobmanager(Service):

    def __init__(self, settings: JobmanagerSettings):
        self.app = procrastinate.App(
            connector=procrastinate.AiopgConnector(
                host=settings.postgres.host,
                port=settings.postgres.port,
                user=settings.postgres.username,
                password=settings.postgres.password.get_secret_value(),
                database=settings.postgres.database,
            ),
        )

    async def setup(self):
        await self.app.open_async()
        if not await self.app.check_connection_async():
            await self.app.schema_manager.apply_schema_async()

    async def teardown(self):
        await self.app.close_async()

    async def new_task(self, data: models.BaseTask):
        task_name, task_cls = None, None
        for _task_cls in models.TASKS_CLASSES:
            if isinstance(data, _task_cls):
                task_name, task_cls = _task_cls.__name__, _task_cls
                break

        if not task_name or not task_cls:
            raise Exception(f"Invalid task type {data.__class__}")

        await self.app.tasks.get(task_name).defer_async(data_dict=utils.jsonable_encoder(data))

    async def create_profile_initial_scan_tasks(self, profile: pnytter.TwitterProfile):
        date_start = profile.joined_datetime.date()

        # One task per month
        tasks = [
            models.ScanProfileTweetsV1(
                profile_id=profile.id,
                date_from=datemonth.date_start,
                date_to=datemonth.date_end_inclusive,
            )
            for datemonth in utils.daterange_by_month(date_start)
        ]

        await utils.async_gather_limited(5, *[
            self.new_task(task) for task in tasks
        ])

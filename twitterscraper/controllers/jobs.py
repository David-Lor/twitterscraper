import asyncio

from twitterscraper.services import Repository, AMQPClient
from twitterscraper.models import BaseJob, JobHistoric
from twitterscraper.utils import get_timestamp


async def enqueue_jobs(*jobs: BaseJob, exchange: str, routingkey: str, persistent: bool):
    await asyncio.gather(
        AMQPClient.get().enqueue(
            exchange=exchange,
            routingkey=routingkey,
            persistent=persistent,
            payloads=[job.json() for job in jobs]
        ),
        save_jobs(*jobs)
    )


async def save_jobs(*jobs: BaseJob):
    repository = Repository.get()
    now_timestamp = get_timestamp()
    async with repository.session_async():
        coroutines = list()
        for job in jobs:
            job_persist = JobHistoric(
                job_id=job.job_id,
                data=job.dict(exclude={"job_id"}),
                timestamp_created=now_timestamp
            )
            coroutines.append(repository.save_object_async(job_persist))

        await asyncio.gather(*coroutines)


async def set_job_finalized(job_id: str):
    repository = Repository.get()
    async with repository.session_async():
        job_persisted = await repository.get_job_historic(job_id)
        if not job_persisted:
            print("Job historic", job_id, "not persisted!")
            return

        job_persisted.timestamp_finalized = get_timestamp()
        await repository.save_object_async(job_persisted)

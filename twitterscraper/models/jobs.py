import pydantic

__all__ = ("BaseJob", "FetchPersistJob", "PersistedReviewJob")


class BaseJob(pydantic.BaseModel):
    job_id: str
    """Unique identifier of the job."""
    job_type: str
    """Constant string for each class of job."""
    job_version: int
    """Schema version of the job. Must be upgraded on breaking changes."""


class _BaseUserTweetsJob(BaseJob):
    userid: str
    """Current username. Should be queried using the userid before creating the job, so we query the updated username"""
    from_timestamp: int
    """Start of the scan time range. Unix timestamp, UTC, inclusive."""
    to_timestamp: int
    """End of the scan time range. Unix timestamp, UTC, exclusive."""


class FetchPersistJob(_BaseUserTweetsJob):
    job_type: str = pydantic.ConstrainedStr("FetchAndPersist")
    job_version: int = pydantic.ConstrainedInt(1)


class PersistedReviewJob(_BaseUserTweetsJob):
    job_type: str = pydantic.ConstrainedStr("PersistedReviewJob")
    job_version: int = pydantic.ConstrainedInt(1)

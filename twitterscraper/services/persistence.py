import os
import asyncio
import subprocess
import contextlib
import contextvars
from typing import *

import wait4it
import sqlmodel
from sqlmodel import Session
from aioify import aioify

from twitterscraper.models.domain import TwitterProfile, TwitterTweet, JobHistoric
from twitterscraper.utils import Singleton


class Repository(Singleton):
    # https://sqlmodel.tiangolo.com/
    get: Callable[..., "Repository"]
    _session_contextvar: contextvars.ContextVar[Optional[Session]]

    def __init__(self, uri: str):
        self._engine = sqlmodel.create_engine(uri)
        self._session_contextvar = contextvars.ContextVar("session", default=None)
        # TODO implement pool_pre_ping & pool_recycle_time

        # Fix for https://github.com/tiangolo/sqlmodel/issues/189#issuecomment-1025190094
        from sqlmodel.sql.expression import Select, SelectOfScalar
        SelectOfScalar.inherit_cache = True  # type: ignore
        Select.inherit_cache = True  # type: ignore

    def _new_session(self) -> Session:
        session = Session(bind=self._engine)
        self._session_contextvar.set(session)
        return session

    def _get_context_session(self) -> Optional[Session]:
        return self._session_contextvar.get()

    def _clear_context_session(self):
        self._session_contextvar.set(None)

    @contextlib.contextmanager
    def session(self) -> Session:
        """Contextmanager that wraps code behind a database transaction (session).
        Any error during the execution rolls back the transaction, so any data saved is not persisted."""
        session = self._get_context_session()
        if session is not None:
            # Another session() contextmanager is already running; let it handle the commit/rollback
            yield session
            return

        session = self._new_session()
        print("New session")
        try:
            yield session
            print("Session Commit")
            session.commit()
        except Exception as ex:
            print("Session Rollback")
            session.rollback()
            raise ex
        finally:
            print("Session Close")
            session.close()
            self._clear_context_session()

    @contextlib.asynccontextmanager
    async def session_async(self) -> Session:
        """Contextmanager that wraps code behind a database transaction (session).
        Any error during the execution rolls back the transaction, so any data saved is not persisted."""
        session = self._get_context_session()
        if session is not None:
            # Another session() contextmanager is already running; let it handle the commit/rollback
            yield session
            return

        session = self._new_session()
        print("New session")
        try:
            yield session
            print("Session Commit")
            await aioify(session.commit)()
        except Exception as ex:
            print("Session Rollback")
            await aioify(session.rollback)()
            raise ex
        finally:
            print("Session Close")
            await aioify(session.close)()
            self._clear_context_session()

    def save_object(self, obj: sqlmodel.SQLModel, flush: bool = False):
        """Save or update any SQLModel object instance"""
        with self.session() as session:
            session.add(obj)
            if flush:
                session.flush([obj])

    async def save_object_async(self, *obj: sqlmodel.SQLModel, flush: bool = False):
        # return await aioify(self.save_object)(obj, flush)
        async with self.session_async() as session:
            await asyncio.gather(*[aioify(session.add)(_obj) for _obj in obj])
            if flush:
                await aioify(session.flush)(obj)

    def delete_object(self, obj: sqlmodel.SQLModel, flush: bool = False):
        with self.session() as session:
            session.delete(obj)
            if flush:
                session.flush([obj])

    async def delete_object_async(self, obj: sqlmodel.SQLModel, flush: bool = False):
        async with self.session_async() as session:
            await aioify(session.delete)(obj)
            if flush:
                await aioify(session.flush)([obj])

    def list_profiles(self) -> List[TwitterProfile]:
        with self.session() as session:
            query = sqlmodel.select(TwitterProfile)
            return session.exec(query).all()

    async def list_profiles_async(self, filter_active_profiles: Optional[bool] = None) -> List[TwitterProfile]:
        async with self.session_async() as session:
            query = sqlmodel.select(TwitterProfile)
            if filter_active_profiles is not None:
                query = query.where(TwitterProfile.active == filter_active_profiles)

            result = await aioify(session.exec)(query)
            return await aioify(result.all)()

    def get_profile_by_userid(self, userid: str) -> TwitterProfile:
        # TODO Deprecate
        with self.session() as session:
            query = sqlmodel.select(TwitterProfile).where(TwitterProfile.userid == userid)
            return session.exec(query).one()

    async def get_profile_by_userid_async(self, userid: str) -> TwitterProfile:
        # TODO Deprecate
        async with self.session_async() as session:
            query = sqlmodel.select(TwitterProfile).where(TwitterProfile.userid == userid)
            result = await aioify(session.exec)(query)
            return await aioify(result.one)()

    async def get_profile_by(self, userid: Optional[str] = None, username: Optional[str] = None) -> TwitterProfile:
        async with self.session_async() as session:
            query = sqlmodel.select(TwitterProfile)
            if userid:
                query = query.where(TwitterProfile.userid == userid)
            if username:
                query = query.where(TwitterProfile.username == username)
            result = await aioify(session.exec)(query)
            return await aioify(result.one)()

    # noinspection PyComparisonWithNone
    async def tweets_iterator(
            self,
            batch_size: int,
            userid: Optional[str] = None,
            username: Optional[str] = None,
            from_ts: Optional[int] = None,
            to_ts: Optional[int] = None,
            filter_active_profiles: Optional[bool] = None,
            filter_active_tweets: Optional[bool] = None,
            tweets_ids: Optional[List[str]] = None,
    ) -> AsyncIterable[List[TwitterTweet]]:
        # TODO Deprecate, iterator seems like no longer needed
        # TODO Any way for making the generator async?
        async with self.session_async() as session:
            query = sqlmodel.select(TwitterTweet).join(TwitterProfile)
            if filter_active_profiles is not None:
                query = query.where(TwitterProfile.active == filter_active_profiles)
            if filter_active_tweets is True:
                query = query.where(TwitterTweet.deletion_detected_timestamp == None)
            elif filter_active_tweets is False:
                query = query.where(TwitterTweet.deletion_detected_timestamp != None)
            if userid is not None:
                query = query.where(TwitterProfile.userid == userid)
            if username is not None:
                query = query.where(TwitterProfile.username == username)
            if from_ts is not None:
                query = query.where(TwitterTweet.timestamp >= from_ts)
            if to_ts is not None:
                query = query.where(TwitterTweet.timestamp < to_ts)
            if tweets_ids is not None:
                # noinspection PyUnresolvedReferences
                query = query.filter(TwitterTweet.tweet_id.in_(tweets_ids))
            query = query.execution_options(stream_results=True)

            result = session.exec(query)
            for partition in result.partitions(batch_size):
                yield partition

    async def get_tweets(
            self,
            **kwargs
    ) -> List[TwitterTweet]:
        tweets = list()
        async for tweets_batch in self.tweets_iterator(batch_size=50, **kwargs):
            tweets.extend(tweets_batch)
        return tweets

    async def get_job_historic(self, job_id: str) -> Optional[JobHistoric]:
        async with self.session_async() as session:
            query = sqlmodel.select(JobHistoric).where(JobHistoric.job_id == job_id)
            return session.exec(query).one_or_none()

    # TODO remove "with self.session..." from everything, since we're returning ORM models, it's always needed on the outside

    async def close(self):
        # TODO implement
        print("Closing Repository...")
        print("Closed Repository")

    @staticmethod
    def run_migrations():
        subprocess.check_output(("alembic", "upgrade", "head"), cwd=os.getcwd())

    @staticmethod
    def generate_migration(name: str):
        subprocess.check_output(("alembic", "revision", "--autogenerate", "-m", name))

    def tcp_wait(self):
        # TODO configurable timeout
        url = self._engine.url
        print("Waiting for TCP port", url)
        wait4it.wait_for(host=url.host, port=url.port, timeout=5)
        print("TCP port ready", url)

    async def tcp_wait_async(self):
        await aioify(self.tcp_wait)()

import asyncio
import datetime
from typing import List

import pydash
import pnytter
import naivemigrations
import sqlalchemy.ext.asyncio
import sqlalchemy.dialects.postgresql

from . import migrations, tables, mapper
from .constants import Const
from ..settings import RepositorySettings
from ..common import Service


class Repository(Service):

    def __init__(self, settings: RepositorySettings):
        self.settings = settings
        self.engine = sqlalchemy.ext.asyncio.create_async_engine(self._get_db_url())
        self.sessionmaker = sqlalchemy.ext.asyncio.async_sessionmaker(self.engine, expire_on_commit=False)
        self.migrationers: List[naivemigrations.AsyncMigrationer] = [
            migrations.TwitterscraperMigrationer(engine=self.engine, sessionmaker=self.sessionmaker)
        ]

    async def setup(self):
        await asyncio.gather(*[m.migrate() for m in self.migrationers])

    async def teardown(self):
        await self.engine.dispose()

    async def get_profile_by_id(self, userid: int) -> tables.Profile | None:
        async with self.sessionmaker() as session:
            query = sqlalchemy.select(tables.Profile).where(tables.Profile.userid == userid).limit(1)
            result = await session.execute(query)
            return result.scalars().one_or_none()

    async def update_profile_last_scan(self, userid: int, date: datetime.date):
        async with self.sessionmaker() as session:
            query = sqlalchemy.update(tables.Profile).where(tables.Profile.userid == userid).\
                values(lastscan_date=date)
            await session.execute(query)
            await session.commit()

    async def write_profile(self, profile: pnytter.TwitterProfile):
        async with self.sessionmaker() as session:
            profile_orm = tables.Profile(
                userid=profile.id,
                username=profile.username,
                enabled=True,
                joined_date=profile.joined_datetime.date(),
            )

            await asyncio.gather(
                # Insert profile if not exists
                session.execute(sqlalchemy.dialects.postgresql.insert(tables.Profile).values([profile_orm.to_dict()]).on_conflict_do_nothing()),

                # Create partition table
                session.execute(sqlalchemy.sql.text(f"""
                CREATE TABLE IF NOT EXISTS {tables.Tweet.__tablename__}_{profile.id}
                PARTITION OF {tables.Tweet.__tablename__}
                FOR VALUES IN ({profile.id});
            """))
            )
            await session.commit()

    async def write_tweets(self, userid: int, tweets: List[pnytter.TwitterTweet]):
        if not tweets:
            return

        async with self.sessionmaker() as session:
            for bulk in pydash.chunk(tweets, Const.POSTGRES_BULK_INSERT_ROWS_LIMIT):
                bulk_orm_dicts = [
                    mapper.pnytter_tweet_to_orm(userid=userid, tweet=tweet).to_dict()
                    for tweet in bulk
                ]

                query = sqlalchemy.dialects.postgresql.insert(tables.Tweet).values(bulk_orm_dicts).\
                    on_conflict_do_nothing(index_elements=[tables.Tweet.tweetid.name, tables.Tweet.userid.name])
                await session.execute(query)

            await session.commit()

    def _get_db_url(self):
        settings = self.settings.postgres
        return f"postgresql+asyncpg://{settings.username}:{settings.password.get_secret_value()}@" \
               f"{settings.host}:{settings.port}/{settings.database}"

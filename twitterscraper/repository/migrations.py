import datetime
from typing import Optional

import sqlalchemy.orm
import sqlalchemy.ext.asyncio
from naivemigrations import AsyncMigrationer

from . import tables
from .constants import Const


class TwitterscraperMigrationer(AsyncMigrationer):

    def __init__(
            self,
            engine: sqlalchemy.ext.asyncio.AsyncEngine,
            sessionmaker: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]
    ):
        self.engine = engine
        self.sessionmaker = sessionmaker

    async def get_last_migration_id(self) -> Optional[str]:
        async with self.sessionmaker() as session:
            async with session.begin():
                conn = await session.connection()
                has_table = await conn.run_sync(
                    lambda _conn: sqlalchemy.inspect(_conn).has_table(tables.Migrations.Twitterscraper.__tablename__)
                )
                if not has_table:
                    print("No migrations table!")  # TODO RM
                    return None

                query = sqlalchemy.select(tables.Migrations.Twitterscraper). \
                    order_by(tables.Migrations.Twitterscraper.migration_id.desc()). \
                    limit(1)

                result = await session.execute(query)
                migration = result.scalars().one_or_none()
                if migration:
                    migration: tables.Migrations.Twitterscraper
                    print("Found last migration ID:", migration.migration_id)  # TODO RM
                    return migration.migration_id
                print("No migrations persisted yet!")  # TODO RM

    async def save_last_migration(self, migration_id: str):
        obj = tables.Migrations.Twitterscraper(
            migration_id=migration_id,
            created_on=datetime.datetime.utcnow(),
        )
        async with self.sessionmaker() as session:
            async with session.begin():
                print("Saving migration", migration_id)  # TODO RM
                session.add_all([obj])

    async def migration_0_add_migration_table(self):
        async with self.sessionmaker() as session:
            await session.execute(sqlalchemy.sql.text(f"""
                        CREATE TABLE {tables.Migrations.Twitterscraper.__tablename__}(
                            {tables.Migrations.Twitterscraper.migration_id.name} VARCHAR({Const.MIGRATIONID_MAXLENGTH}) PRIMARY KEY,
                            {tables.Migrations.Twitterscraper.created_on.name} TIMESTAMP NOT NULL
                        );
                    """))
            await session.commit()

    async def migration_1_add_profiles_table(self):
        async with self.sessionmaker() as session:
            await session.execute(sqlalchemy.sql.text(f"""
                CREATE TABLE {tables.Profile.__tablename__}(
                    {tables.Profile.userid.name} BIGINT PRIMARY KEY NOT NULL,
                    {tables.Profile.username.name} VARCHAR({Const.TWITTER_USERNAME_MAXLENGTH}) UNIQUE NOT NULL,
                    {tables.Profile.enabled.name} BOOLEAN NOT NULL DEFAULT true,
                    {tables.Profile.archiveorg_enabled.name} BOOLEAN NOT NULL DEFAULT true,
                    {tables.Profile.joined_date.name} DATE NOT NULL,
                    {tables.Profile.lastscan_date.name} DATE DEFAULT NULL
                );
            """))
            await session.commit()

    async def migration_2_add_tweets_table(self):
        async with self.sessionmaker() as session:
            await session.execute(sqlalchemy.sql.text(f"""
                CREATE TABLE {tables.Tweet.__tablename__}(
                    {tables.Tweet.tweetid.name} BIGINT NOT NULL,
                    {tables.Tweet.userid.name} BIGINT NOT NULL,
                    {tables.Tweet.published_on.name} TIMESTAMP NOT NULL,
                    {tables.Tweet.data.name} JSON NOT NULL,
                    {tables.Tweet.deletion_detected_on.name} TIMESTAMP DEFAULT NULL,
                    {tables.Tweet.archiveorg_url.name} VARCHAR({Const.ARCHIVEORG_URL_MAXLENGTH}) DEFAULT NULL,
                    {tables.Tweet.archiveorg_scheduled.name} BOOLEAN DEFAULT FALSE,
                    
                    PRIMARY KEY ({tables.Tweet.userid.name}, {tables.Tweet.tweetid.name}),
                    CONSTRAINT fk_{tables.Tweet.userid.name}
                        FOREIGN KEY ({tables.Tweet.userid.name})
                            REFERENCES {tables.Profile.__tablename__}({tables.Profile.userid.name})
                                ON DELETE CASCADE
                ) PARTITION BY LIST({tables.Profile.userid.name});
            """))
            # PRIMARY KEY ({tables.Tweet.published_on.name}, {tables.Tweet.tweetid.name}),
            # ) PARTITION BY RANGE({tables.Tweet.created_on.name}, {tables.Tweet.userid.name});
            await session.commit()

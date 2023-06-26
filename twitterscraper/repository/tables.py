import datetime

import sqlalchemy.ext.asyncio
from sqlalchemy.orm import Mapped

from .constants import Const


class _Base(sqlalchemy.ext.asyncio.AsyncAttrs, sqlalchemy.orm.DeclarativeBase):

    def to_dict(self):
        """Return a dict with all the fields from the instance. Fields with null values are ignored."""
        mapper = sqlalchemy.orm.object_mapper(self)
        d = dict()

        for column in mapper.columns:
            column = column.key
            value = getattr(self, column)
            if value is not None:
                d[column] = value

        return d


class Migrations:

    class Twitterscraper(_Base):
        __tablename__ = "twitterscraper_migrations"

        migration_id: Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.String(Const.MIGRATIONID_MAXLENGTH), primary_key=True)
        created_on: Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column()


class Profile(_Base):
    __tablename__ = "twitterscraper_profiles"

    userid: Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BIGINT, primary_key=True)
    username: Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.String(Const.TWITTER_USERNAME_MAXLENGTH), unique=True, nullable=False)
    enabled: Mapped[bool] = sqlalchemy.orm.mapped_column(default=True, nullable=False)
    archiveorg_enabled: Mapped[bool] = sqlalchemy.orm.mapped_column(default=True, nullable=False)
    joined_date: Mapped[datetime.date] = sqlalchemy.orm.mapped_column(nullable=False)
    lastscan_date: Mapped[datetime.date] = sqlalchemy.orm.mapped_column(nullable=True, default=None)


class Tweet(_Base):
    __tablename__ = "twitterscraper_tweets"

    tweetid: Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BIGINT, primary_key=True)
    userid: Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BIGINT, sqlalchemy.ForeignKey(f"{Profile.__tablename__}.{Profile.userid.name}"), primary_key=True)
    published_on: Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column()
    data: Mapped[dict] = sqlalchemy.orm.mapped_column(sqlalchemy.JSON(), nullable=False)
    deletion_detected_on: Mapped[datetime.datetime | None] = sqlalchemy.orm.mapped_column(nullable=True, default=None)
    archiveorg_url: Mapped[str | None] = sqlalchemy.orm.mapped_column(sqlalchemy.String(Const.ARCHIVEORG_URL_MAXLENGTH), nullable=True, default=None)
    archiveorg_scheduled: Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False, default=False)

import datetime
from typing import *

import pydantic
from sqlmodel import SQLModel, Field, Relationship, Column, JSON

__all__ = ("TwitterProfile", "TwitterTweet", "JobHistoric", "TweetScanStatus")


class TwitterProfile(SQLModel, table=True):
    __tablename__ = "profiles"

    # Columns
    id: Optional[int] = Field(default=None, primary_key=True)  # autoincrement
    username: str = Field(sa_column_kwargs=dict(unique=True))
    userid: str = Field(sa_column_kwargs=dict(unique=True))
    joined_date: datetime.date = Field()
    active: bool = Field(default=True)
    last_scan_timestamp: Optional[int] = Field(default=None, gt=0)

    # Relationships
    tweets: List["TwitterTweet"] = Relationship(back_populates="profile")


class TwitterTweet(SQLModel, table=True):
    __tablename__ = "tweets"

    # Columns
    tweet_id: str = Field(primary_key=True)
    text: str = Field()
    timestamp: int = Field(gt=0, index=True)
    is_reply: bool = Field()
    last_review_timestamp: Optional[int] = Field(default=None, gt=0)
    deletion_detected_timestamp: Optional[int] = Field(default=None, gt=0)

    # Relationships
    profile_id: int = Field(foreign_key=f"{TwitterProfile.__tablename__}.id")
    profile: TwitterProfile = Relationship(back_populates="tweets")

    @property
    def url(self):
        if not self.profile or not self.profile.username:
            raise AttributeError("Cannot access profile username")
        return f"https://www.twitter.com/{self.profile.username}/status/{self.tweet_id}"


class JobHistoric(SQLModel, table=True):
    __tablename__ = "jobs_historic"

    # Columns
    job_id: str = Field(primary_key=True)
    data: Dict = Field(sa_column=Column(JSON, nullable=False))
    timestamp_created: int = Field(gt=0, nullable=False)
    timestamp_finalized: Optional[int] = Field(default=None, gt=0)

    class Config:
        arbitrary_types_allowed = True


class TweetScanStatus(pydantic.BaseModel):
    tweet_id: str
    exists: bool
    timestamp: Optional[int]

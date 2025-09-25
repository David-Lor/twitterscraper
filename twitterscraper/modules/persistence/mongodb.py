import asyncio
import datetime
import pymongo.errors
from motor.motor_asyncio import AsyncIOMotorClient
from .base import BaseRepository, TweetFilters
from ...models.twitter import Tweet
from ...models.persistence import PartialPersistTweetDeleted, TweetArchive
from ...settings import Settings
from ...logger import logger


class Const:
    IdField = "_id"
    TweetIdRmField = "tweet_id"
    DataField = "data"
    WhenArchivedField = "when_archived"
    WhenDeletedField = "when_deleted"
    ArchivesField = "archives"
    CookiesDoc = "cookies"
    UserAgentsDoc = "userAgents"


class MongoRepository(BaseRepository):

    def __init__(self):
        self.settings = Settings.get().persistence.mongodb
        self.client = AsyncIOMotorClient(self.settings.uri_with_auth)
        self.database = self.client[self.settings.database]

    async def save_tweets(self, *tweets: Tweet):
        if not tweets:
            return

        coroutines = list()
        for tweet in tweets:
            collection = self.get_collection_user_tweets(tweet.when_published)
            doc = tweet.model_dump(mode="python")
            doc[Const.IdField] = doc.pop(Const.TweetIdRmField)
            coroutines.append(self.insert_or_ignore_exists(collection, doc))

        results = await asyncio.gather(*coroutines)

        insert_count = sum(1 for result in results if result)
        if insert_count:
            logger.bind(insert_count=insert_count, input_count=len(tweets)).info("Saved tweets in Mongo")
        else:
            logger.bind(input_count=len(tweets)).debug("No new tweets to save in Mongo")

    async def get_tweet(self, tweet_id: int) -> Tweet | None:
        with logger.contextualize(tweet_id=tweet_id):
            for collection in await self.get_existing_user_tweets_collections():
                with logger.contextualize(collection=collection.name):
                    result = await collection.find_one({Const.IdField: tweet_id})
                    if result:
                        logger.trace("Tweet found in Mongo")
                        return Tweet.model_validate(result)

        logger.trace("Tweet not found in Mongo")
        return None

    async def iterate_all_tweets_ids(self, filters: TweetFilters | None = None):
        filters = self.tweetfilters_to_mongo_filters(filters)
        for collection in await self.get_existing_user_tweets_collections():
            results_cursor = collection.find(
                filters,
                {Const.IdField: 1},
            )
            async for document in results_cursor:
                yield document[Const.IdField]

    async def mark_tweet_deleted(self, tweet_id: int, deleted_on: datetime.datetime):
        update = PartialPersistTweetDeleted(
            when_deleted=deleted_on
        ).model_dump()
        for collection in await self.get_existing_user_tweets_collections():
            result = await collection.update_one(
                filter={Const.IdField: tweet_id},
                update={"$set": update},
            )
            if result.modified_count:
                logger.bind(tweet_id=tweet_id, collection=collection.name).debug("Tweet marked as Deleted in Mongo")
                break

    async def unmark_tweet_deleted(self, tweet_id: int):
        for collection in await self.get_existing_user_tweets_collections():
            result = await collection.update_one(
                filter={Const.IdField: tweet_id},
                update={"$unset": {"when_deleted": 1}},
            )
            if result.modified_count:
                logger.bind(tweet_id=tweet_id, collection=collection.name).debug("Removed tweet deletion mark in Mongo")
                break

    async def mark_tweet_archived(self, tweet_id: int, archiver_name: str, archive_url: str, archive_time: datetime.datetime):
        archive_entry_doc = TweetArchive(
            archiver=archiver_name,
            url=archive_url,
            archived_on=archive_time,
        ).model_dump()

        for collection in await self.get_existing_user_tweets_collections():
            result = await collection.update_one(
                filter={Const.IdField: tweet_id},
                update={"$push": {Const.ArchivesField: archive_entry_doc}}
            )
            if result.modified_count:
                logger.bind(tweet_id=tweet_id, collection=collection.name).debug("Tweet marked as Archived in Mongo")
                break

    async def get_twitter_cookies(self) -> list[dict]:
        return await self.get_kv_data_field(Const.CookiesDoc)

    async def get_useragents(self) -> list[str]:
        return await self.get_kv_data_field(Const.UserAgentsDoc)

    async def get_kv_doc(self, k: str) -> dict:
        return await self.get_collection_kv().find_one({Const.IdField: k})

    async def get_kv_data_field(self, k: str):
        doc = await self.get_kv_doc(k)
        return doc[Const.DataField]

    async def get_existing_user_tweets_collections(self):
        collections_names = await self.database.list_collection_names()
        # TODO Format from settings
        return [
            self.database[collection_name] for collection_name in collections_names
            if collection_name.startswith("tweets-")
        ]

    def get_collection_user_tweets(self, date: datetime.date):
        # TODO Format from settings. Only allow aggregation by date
        return self.database[f"tweets-{date.year}-{date.month:02}"]

    def get_collection_kv(self):
        return self.database[self.settings.collections.kv]

    @classmethod
    async def insert_or_ignore_exists(cls, collection, doc) -> bool:
        try:
            await collection.insert_one(doc)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    # noinspection PySimplifyBooleanCheck
    @staticmethod
    def tweetfilters_to_mongo_filters(tweet_filters: TweetFilters | None) -> dict:
        if tweet_filters:
            mongo_filters = []
            if tweet_filters.deleted is True:
                mongo_filters.append({Const.WhenDeletedField: {"$ne": None}})
            if tweet_filters.deleted is False:
                mongo_filters.append({Const.WhenDeletedField: None})
            if tweet_filters.archived is True:
                mongo_filters.append({Const.WhenArchivedField: {"$ne": None}})
            if tweet_filters.archived is False:
                mongo_filters.append({Const.WhenArchivedField: None})

            if mongo_filters:
                return {"$and": mongo_filters}

        return {}

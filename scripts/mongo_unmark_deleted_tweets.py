import asyncio
from twitterscraper.modules.persistence.mongodb import MongoRepository
from twitterscraper.settings import Settings

SRC_COLLECTION_STARTSWITH = "tweets-"


async def amain():
    Settings.load_from_file()
    mongo = MongoRepository()
    query = {
        "when_deleted": {"$ne": None}
    }
    update = {
        "$set": {"when_deleted": None}
    }

    for collection_name in await mongo.database.list_collection_names():
        if collection_name.startswith(SRC_COLLECTION_STARTSWITH):
            result = await mongo.database[collection_name].update_many(query, update)
            print(collection_name, "Updated", result.modified_count, "docs")


if __name__ == '__main__':
    asyncio.run(amain())

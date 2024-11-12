import asyncio
import datetime
from twitterscraper.modules.persistence.mongodb import MongoRepository
from twitterscraper.settings import Settings

SRC_COLLECTION_STARTSWITH = "tweets-"
DST_COLLECTION_NAME = "tweets-{year}-{month}"


async def amain():
    Settings.load_from_file()
    mongo = MongoRepository()

    all_docs = list()
    for collection_name in await mongo.database.list_collection_names():
        if collection_name.startswith(SRC_COLLECTION_STARTSWITH):
            docs = list()
            async for doc in mongo.database[collection_name].find({}):
                docs.append(doc)

            print(collection_name, "found", len(docs), "docs")
            all_docs.extend(docs)

    print("Found", len(all_docs), "total docs")

    collections_docs: dict[str, list] = dict()
    for doc in all_docs:
        date: datetime.datetime = doc["when_published"]
        target_collection_name = DST_COLLECTION_NAME.format(
            year=date.year,
            month=str(date.month).zfill(2),
        )

        try:
            collections_docs[target_collection_name].append(doc)
        except KeyError:
            collections_docs[target_collection_name] = [doc]

    # print("Relation of collections and documents to insert:",
    #       json.dumps({coll: len(docs) for coll, docs in collections_docs.items()}, indent=2))
    for collection_name, docs in collections_docs.items():
        print(collection_name, "Inserting", len(docs), "docs")
        mongo.database[collection_name].insert_many(docs)


if __name__ == '__main__':
    asyncio.run(amain())

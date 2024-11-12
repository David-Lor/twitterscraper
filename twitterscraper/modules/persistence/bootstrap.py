from .base import BaseRepository
from .mongodb import MongoRepository
from ...settings import Settings


class Repository:

    @classmethod
    def get(cls) -> BaseRepository:
        return cls.__instance__

    @classmethod
    async def setup(cls) -> BaseRepository:
        settings = Settings.get().persistence
        if settings.mongodb:
            repository = MongoRepository()
        else:
            raise Exception("No persistence configured")

        cls.__instance__ = repository
        return cls.get()

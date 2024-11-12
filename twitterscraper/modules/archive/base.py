import abc
import datetime

ArchivedResult = tuple[str, datetime.datetime]


class BaseArchiver(abc.ABC):

    @property
    @abc.abstractmethod
    def archiver_name(self):
        pass

    async def get_or_save(self, url: str) -> ArchivedResult:
        archived_url = await self.get_latest(url)
        if not archived_url:
            archived_url = await self.save(url)
        return archived_url

    @abc.abstractmethod
    async def save(self, url: str) -> ArchivedResult:
        pass

    @abc.abstractmethod
    async def get_latest(self, url: str) -> ArchivedResult | None:
        pass

import re
import asyncio
from .base import BaseArchiver, ArchivedResult
from ..http import http_client, AsyncClient
from ...settings import Settings
from ...utils import html_parse, parse_datetime
from ...logger import logger

MEMENTO_PATTERN = r'<(https?://[^\s>]+)>;\s*rel="([^"]+)"(?:;\s*(\S+)="([^"]+)")?'


class ArchiveToday(BaseArchiver):

    # TODO Revisar, 429 continuamente

    def __init__(self):
        self.settings = Settings.get().archivers.archivetoday

    async def save(self, url: str) -> ArchivedResult:
        async with http_client(self.settings.http, follow_redirects=True) as client:
            params = dict(url=url)
            r = await client.get("https://archive.is/submit/", params=params)
            r.raise_for_status()

            link_header = r.headers.get("Link")
            if link_header:
                return self._parse_memento(link_header)

            refresh_header = r.headers.get("Refresh")  # example: '0;url=https://archive.is/wip/6Ry4C'
            refresh_url = refresh_header.split("url=")[-1]
            return await self._wait_for_archived_url(client, refresh_url)

    async def get_latest(self, url: str) -> ArchivedResult | None:
        return await self.save(url)

    async def _wait_for_archived_url(self, client: AsyncClient, refresh_url: str) -> ArchivedResult:
        # TODO Add Retries limit
        while True:
            await asyncio.sleep(10)

            logger.bind(archive_refresh_url=refresh_url).debug("Waiting for URL archived")
            r = await client.get(refresh_url)
            r.raise_for_status()

            link = r.headers.get("Link")
            if link:
                logger.bind(archive_url=link).debug("URL archived successfully, parsing memento")

                archive_result = self._parse_memento(link)
                logger.bind(archive_result=archive_result).info("ArchiveResult")
                return archive_result

    @staticmethod
    def _parse_memento(data: str) -> ArchivedResult:
        matches = re.findall(MEMENTO_PATTERN, data)
        for match in matches:
            url, rel, param_name, param_value = match
            if param_name == "datetime" and param_value:
                # datetime example: "Sat, 16 Nov 2024 20:09:01 GMT"
                snapshot_datetime = parse_datetime(param_value)
                return url, snapshot_datetime

        raise ValueError("No memento result found")

    @property
    def archiver_name(self):
        return "archive.today"

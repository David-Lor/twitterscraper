import asyncio

import waybackpy
import waybackpy.exceptions

USER_AGENT = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"


class Archiveorg:
    @classmethod
    async def archive_url_or_get_latest(cls, url: str) -> str:
        archive_url = await cls.get_latest_archive(url)
        if not archive_url:
            archive_url = await cls.archive_url_force(url)

        return archive_url

    @classmethod
    async def archive_url_force(cls, url: str) -> str:
        client = waybackpy.WaybackMachineSaveAPI(
            url=url,
            user_agent=USER_AGENT,
        )

        return await asyncio.get_event_loop().run_in_executor(None, lambda: client.save())

    @classmethod
    async def get_latest_archive(cls, url: str) -> str | None:
        # TODO Search tweet by regex, considering username may have changed
        client = waybackpy.WaybackMachineCDXServerAPI(
            url=url,
            user_agent=USER_AGENT,
        )

        try:
            result = await asyncio.get_event_loop().run_in_executor(None, lambda: client.near())
            return result.archive_url
        except waybackpy.exceptions.NoCDXRecordFound:
            return None

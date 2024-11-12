import random
import asyncio
import contextlib
from httpx import AsyncClient
from .persistence.bootstrap import Repository
from ..settings import HttpSettings

__all__ = ["http_client", "AsyncClient"]


@contextlib.asynccontextmanager
async def http_client(settings: HttpSettings, with_twitter_cookies: bool = False, **kwargs) -> AsyncClient:
    repository = Repository.get()
    all_twitter_cookies, all_useragents = await asyncio.gather(
        repository.get_twitter_cookies(),
        repository.get_useragents(),
    )

    cookies = None
    if with_twitter_cookies:
        cookies = random.choice(all_twitter_cookies)

    useragent = random.choice(all_useragents)
    headers = {
        "User-Agent": useragent,
    }
    proxy = (random.choice(settings.proxies) or None) if settings.proxies else None
    timeout = settings.timeout

    async with AsyncClient(proxy=proxy, timeout=timeout, headers=headers, **kwargs) as client:
        client.cookies = cookies
        yield client

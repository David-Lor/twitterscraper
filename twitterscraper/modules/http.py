import random
import asyncio
import contextlib
from httpx import AsyncClient, Request, Response
from .persistence.bootstrap import Repository
from ..settings import HttpSettings
from ..logger import logger

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

    async with AsyncClient(proxy=proxy, timeout=timeout, headers=headers, event_hooks=EVENT_HOOKS, **kwargs) as client:
        client.cookies = cookies
        # noinspection PyTypeChecker
        yield client


async def log_request(request: Request):
    logger.bind(
        http_req_method=request.method,
        http_req_url=str(request.url),
        http_req_headers=dict(request.headers),
        http_req_body=request.content.decode(),
    ).trace("HTTP Request")


async def log_response(response: Response):
    logger.bind(
        http_req_method=response.request.method,
        http_req_url=str(response.url),
        http_res_status_code=response.status_code,
        http_res_body=(await response.aread()).decode(),
    ).trace("HTTP Response")


EVENT_HOOKS = dict(
    request=[log_request],
    response=[log_response],
)

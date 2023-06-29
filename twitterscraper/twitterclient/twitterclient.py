import asyncio
import datetime
from typing import Sequence

import pnytter

from ..common import Service
from ..settings import NitterSettings


class Twitterclient(Service):

    def __init__(self, settings: NitterSettings):
        self.pnytter = pnytter.Pnytter(
            nitter_instances=settings.instances,
            request_timeout=settings.request_timeout_seconds,
            beautifulsoup_parser="lxml",
        )

    async def find_user(self, username: str):
        return await asyncio.get_event_loop().run_in_executor(None, lambda: self.pnytter.find_user(
            username=username,
        ))

    async def get_tweet(self, tweet_id: int, search_all_instances: bool = False):
        return await asyncio.get_event_loop().run_in_executor(None, lambda: self.pnytter.get_tweet(
            tweet_id=tweet_id,
            search_all_instances=search_all_instances,
        ))

    async def get_user_tweets_list(self, username: str, date_from: datetime.date, date_to_exc: datetime.date):
        return await asyncio.get_event_loop().run_in_executor(None, lambda: self.pnytter.get_user_tweets_list(
            username=username,
            filter_from=date_from,
            filter_to=date_to_exc,
        ))

    async def ensure_tweets_are_deleted(self, tweets_ids: Sequence[int]) -> set[int]:
        final_deleted_tweets_ids = set()

        async def __detect(tweet_id: int):
            # TODO Soporte a repetir para soportar el proxy balanceado, o repetir el hostname de nitter con alguna variacion que no se tenga en cuenta para el search_all_instances
            tweet_found = await self.get_tweet(
                tweet_id=tweet_id,
                search_all_instances=True,
            )
            if not tweet_found:
                final_deleted_tweets_ids.add(tweet_id)

        await asyncio.gather(*[
            __detect(tweetid) for tweetid in tweets_ids
        ])
        return final_deleted_tweets_ids

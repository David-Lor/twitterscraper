import asyncio
from typing import *

import aio_pika

from twitterscraper.utils import Singleton


ConsumerCallback = Callable[[bytes], Awaitable[None]]


class AMQPClient(Singleton):
    get: Callable[..., "AMQPClient"]
    _connection: aio_pika.Connection
    _channel: aio_pika.Channel

    def __init__(self, uri: str):
        self._uri = uri
        self._exchanges = dict()
        # noinspection PyTypeChecker
        self._connection, self._channel = None, None

    async def connect(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        print("AMQP Connecting...")
        if loop is None:
            loop = asyncio.get_event_loop()
        self._connection = await aio_pika.robust_connection.connect_robust(self._uri, loop=loop)
        self._channel = await self._connection.channel()
        print("AMQP Connected")

    async def get_exchange(self, exchange_name: str) -> aio_pika.Exchange:
        # TODO implement locks for possible concurrent accesses
        exchange = self._exchanges.get(exchange_name)
        if exchange is not None:
            return exchange

        if not exchange_name:
            exchange = self._channel.default_exchange
        else:
            # TODO not tested
            exchange = await self._channel.get_exchange(exchange_name, ensure=True)

        self._exchanges[exchange_name] = exchange
        return exchange

    async def enqueue(
            self,
            exchange: str,
            routingkey: str,
            persistent: bool,
            payloads: List[Union[bytes, str]]
    ):
        exchange = await self.get_exchange(exchange)
        coroutines = list()

        # TODO run in batches
        # TODO wrap in AMQP transaction
        for payload in payloads:
            if isinstance(payload, str):
                payload = payload.encode("utf-8")

            message = aio_pika.Message(body=payload)
            if persistent:
                message.delivery_mode = aio_pika.DeliveryMode(aio_pika.DeliveryMode.PERSISTENT)

            coroutines.append(exchange.publish(
                message=message,
                routing_key=routingkey
            ))
            print(f"AMQP TX exchange={exchange} routingkey={routingkey} payload={payload}")

        await asyncio.gather(*coroutines)

    async def consume(self, queue: str, callback: ConsumerCallback, workers: int, msg_limit: Optional[int] = None):
        """Async blocking consume"""
        print("Consuming", queue)
        queue = await self._channel.get_queue(queue, ensure=True)
        await self._channel.set_qos(prefetch_count=workers)
        consumed_msgs = 0 if msg_limit is not None else None

        async def _message_handler_task(message: aio_pika.IncomingMessage):
            try:
                print("AMQP RX", message.body)
                await callback(message.body)
            except Exception as ex:
                print("AMQP RX Callback exception:", ex)
                message.nack()
                raise ex
            else:
                message.ack()

        async with queue.iterator() as queue_iter:
            async for _message in queue_iter:
                if msg_limit is None:
                    # Worker execution mode
                    asyncio.create_task(_message_handler_task(_message))
                else:
                    # Run-once execution mode
                    await _message_handler_task(_message)
                    consumed_msgs += 1
                    if consumed_msgs >= msg_limit:
                        break

    async def close(self):
        print("AMQP Closing...")
        if self._channel:
            await self._channel.close()
        if self._connection:
            await self._connection.close()
        print("AMQP Closed")

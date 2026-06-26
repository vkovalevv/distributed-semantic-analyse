import asyncio
import aio_pika


class Worker:
    def __init__(self):
        self._connection: aio_pika.abc.AbstractConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(
            "amqp://guest:guest@localhost:5672/"
        )
        self._channel = await self._connection.channel()

        self._fresh_tasks = await self._channel.declare_queue(
            "fresh_tasks", durable=True
        )
        self._completed_tasks = await self._channel.declare_queue(
            "completed_tasks", durable=True
        )
        await self._fresh_tasks.consume(self.on_work)

    async def on_work(self, message: aio_pika.abc.AbstractIncomingMessage):
        async with message.process():
            await asyncio.sleep(0.0001)
            await self._channel.default_exchange.publish(
                message=aio_pika.Message(body=message.body),
                routing_key="completed_tasks",
            )

async def main():
    w = Worker()
    await w.connect()
    await asyncio.Future()

asyncio.run(main())
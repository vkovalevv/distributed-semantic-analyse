import os
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
from sentence_transformers import SentenceTransformer
import torch
torch.set_num_threads(1)
torch.set_num_interop_threads(1)
import asyncio
import aio_pika
import json



class Worker:
    def __init__(self):
        self._connection: aio_pika.abc.AbstractConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._model: SentenceTransformer | None = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(
            "amqp://guest:guest@localhost:5672/"
        )
        self._channel = await self._connection.channel()
        self._model = SentenceTransformer('all-MiniLM-L6-v2')
        await self._channel.set_qos(prefetch_count=1)
        self._fresh_tasks = await self._channel.declare_queue(
            "fresh_tasks", durable=True
        )
        self._completed_tasks = await self._channel.declare_queue(
            "completed_tasks", durable=True
        )
        await self._fresh_tasks.consume(self.on_work)

    async def on_work(self, message: aio_pika.abc.AbstractIncomingMessage):
        async with message.process():
            payload = json.loads(message.body)
            texts = [item[1] for item in payload['items']]
            vecs = self._model.encode(texts)
            await self._channel.default_exchange.publish(
                message=aio_pika.Message(body=json.dumps(
                    {'shape': f'{vecs.shape}'}).encode()),
                routing_key="completed_tasks",
            )


async def main():
    w = Worker()
    await w.connect()
    await asyncio.Future()

asyncio.run(main())

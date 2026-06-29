import asyncio, json
import numpy as np
import aio_pika

from datasets import load_dataset

M = 5000
G = 50


async def prepare_corpus() -> list[tuple[str, str]]:
    ds = load_dataset("fancyzhx/ag_news", split="train")
    texts = ds["text"][:M]

    return [(f"c{i}", text) for i, text in enumerate(texts)]


async def main():
    corpus = await prepare_corpus()
    id2text = {cid: txt for cid, txt in corpus}

    connection = await aio_pika.connect_robust('amqp://guest:guest@localhost:5672/')
    channel = await connection.channel()
    fresh_tasks = await channel.declare_queue('fresh_tasks', durable=True)
    completed_tasks = await channel.declare_queue('completed_tasks', durable=True)

    await fresh_tasks.purge()
    await completed_tasks.purge()

    collected = {}
    done = asyncio.Event()

    async def on_result(message:aio_pika.abc.AbstractIncomingMessage):
        for cid, vec in json.loads(message.body)['results']:
            collected[cid] = vec
        if len(collected) == M:
            done.set()
    
    await completed_tasks.consume(on_result)

    for i in range(0, len(corpus), G):
        await channel.default_exchange.publish(
            message=aio_pika.Message(
                body = json.dumps({'items': corpus[i:i+G]}).encode()
            ),
            routing_key='fresh_tasks'
        )

    await done.wait()

    vectors = np.array([collected[cid] for cid, _ in corpus], dtype=np.float32)
    meta = [{'id': cid, 'text': id2text[cid]} for cid, _ in corpus]
    np.save('vectors.npy', vectors)
    with open('meta.json', 'w', encoding='utf-8') as f: 
        json.dump(meta, f, ensure_ascii=False)
    print('indexed', vectors.shape)

asyncio.run(main())
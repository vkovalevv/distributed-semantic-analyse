import asyncio, time, json

import aio_pika

M = 500
G = 50                                                                

async def prepare_corpus() -> list[tuple[str,str]]:
    return [(i, 'dummy text') for i in range(M)]

async def warm_up(channel)   -> None:
    return  


async def run_once(channel:aio_pika.abc.AbstractChannel, corpus) -> float:
    fresh_tasks = await channel.declare_queue(name='fresh_tasks', durable=True)
    completed_tasks = await channel.declare_queue(name='completed_tasks', durable=True)
    counter=0
    target=(len(corpus) + G - 1)//G
    done = asyncio.Event()

    async def on_result(message):
        nonlocal counter
        counter += 1 
        if counter == target:
            done.set()

    await completed_tasks.consume(on_result, no_ack=True)

    t0 = time.perf_counter()    
    for i in range(0,len(corpus),G):
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps({'items':corpus[i:i+G]}).encode()),
            routing_key='fresh_tasks',
        )
        
    await done.wait()
    t1 = time.perf_counter()
    return t1-t0
    
async def main():
    connection = await aio_pika.connect_robust('amqp://guest:guest@localhost:5672/')
    async with connection:
        channel = await connection.channel()
        corpus = await prepare_corpus()
        await warm_up(channel)
        elapsed = await run_once(channel, corpus)
        print(f'T={elapsed:.3f}, throughput={len(corpus)/elapsed}')
    ...
asyncio.run(main())
import asyncio
import sys
import json
import time
import csv
import statistics
import aio_pika

from datasets import load_dataset

n_workers = int(sys.argv[1])
base = 2000
M = base * n_workers
G = 50


async def prepare_corpus() -> list[tuple[str, str]]:
    ds = load_dataset("fancyzhx/ag_news", split="train")
    texts = ds['text'][:M]

    return [(f'c{i}',t) for i,t in enumerate(texts)]

async def warm_up(channel, corpus) -> None:
    await run_once(channel, corpus)


async def run_once(channel: aio_pika.abc.AbstractChannel, corpus) -> float:
    fresh_tasks = await channel.declare_queue(name="fresh_tasks", durable=True)
    completed_tasks = await channel.declare_queue(name="completed_tasks", durable=True)
    await fresh_tasks.purge()
    await completed_tasks.purge()
    counter = 0
    target = (len(corpus) + G - 1) // G
    done = asyncio.Event()

    async def on_result(message):
        nonlocal counter
        counter += 1
        if counter == target:
            done.set()

    consumer_tag = await completed_tasks.consume(on_result, no_ack=True)

    t0 = time.perf_counter()
    for i in range(0, len(corpus), G):
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps({"items": corpus[i : i + G]}).encode()),
            routing_key="fresh_tasks",
        )

    await done.wait()
    t1 = time.perf_counter()
    await completed_tasks.cancel(consumer_tag)
    return t1 - t0


async def run_experiment(channel, corpus, K=5, n_workers=3, csv_path="results_weak.csv"):
    await warm_up(channel, corpus)
    rows = []
    for run in range(K):
        elapsed = await run_once(channel, corpus)
        thr = len(corpus) / elapsed
        rows.append(
            {
                "n_workers": n_workers,
                "M": len(corpus),
                "G": G,
                "run": run,
                "time_s": elapsed,
                "throughput": thr,
            }
        )

    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["n_workers", "M", "G", "run", "time_s", "throughput"]
        )
        writer.writeheader()
        writer.writerows(rows)

    throughputs = [row["throughput"] for row in rows]
    mean = statistics.mean(throughputs)
    stdev = statistics.stdev(throughputs)
    print(f"throughput: mean={mean:.1f}, std={stdev:.1f} (K={K})")
    return


async def main():
    
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/")
    async with connection:
        channel = await connection.channel()
        corpus = await prepare_corpus()
        await run_experiment(channel, corpus, n_workers=n_workers)


asyncio.run(main())

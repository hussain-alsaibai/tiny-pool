"""Benchmarks for tiny-pool. Run with `python bench_tiny_pool.py`."""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import tiny_pool as tp


def bench(name, fn, n=10_000):
    fn()
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    dt = (time.perf_counter() - t0) / n * 1e6
    print(f"  {name:40s} {dt:10.3f} µs/op")


def main():
    print("== tiny-pool benchmarks (n=10,000) ==")

    # ThreadPool: submit + future.result() cost (work is trivial)
    with tp.ThreadPool(max_workers=4) as p:
        bench("ThreadPool submit+result()", lambda: p.submit(lambda: 1).result(), n=2_000)

    # AsyncPool: async overhead
    async def runner():
        async with tp.AsyncPool(max_workers=4) as p:
            async def one():
                return 1
            t0 = time.perf_counter()
            n = 5_000
            for _ in range(n):
                await p.submit(one)
            dt = (time.perf_counter() - t0) / n * 1e6
            print(f"  {'AsyncPool submit+await':40s} {dt:10.3f} µs/op")

    asyncio.run(runner())


if __name__ == "__main__":
    main()

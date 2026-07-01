"""Tests for tiny-pool. Run with `python test_tiny_pool.py`. Stdlib only."""

import asyncio
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import tiny_pool as tp


# ---------------------------------------------------------------------------
# ThreadPool
# ---------------------------------------------------------------------------


class TestThreadPool(unittest.TestCase):
    def test_basic_submit(self):
        with tp.ThreadPool(max_workers=2) as p:
            f = p.submit(lambda x: x * 2, 21)
            self.assertEqual(f.result(timeout=2), 42)

    def test_submit_many(self):
        with tp.ThreadPool(max_workers=4) as p:
            futs = [p.submit(lambda i=i: i + 1) for i in range(20)]
            results = [f.result(timeout=2) for f in futs]
            self.assertEqual(results, list(range(1, 21)))

    def test_submit_with_args_and_kwargs(self):
        with tp.ThreadPool(max_workers=1) as p:
            f = p.submit(lambda a, b, *, c: a + b + c, 1, 2, c=3)
            self.assertEqual(f.result(timeout=2), 6)

    def test_submit_preserves_exception(self):
        with tp.ThreadPool(max_workers=1) as p:
            def boom():
                raise ValueError("nope")

            f = p.submit(boom)
            with self.assertRaises(ValueError) as cm:
                f.result(timeout=2)
            self.assertEqual(str(cm.exception), "nope")

    def test_map_preserves_order(self):
        with tp.ThreadPool(max_workers=4) as p:
            results = p.map(lambda x: x ** 2, range(10))
            self.assertEqual(results, [i * i for i in range(10)])

    def test_map_single_arg(self):
        with tp.ThreadPool(max_workers=2) as p:
            # map() invokes fn(item) for each item — one positional arg only.
            results = p.map(lambda x: x + 100, [1, 2, 3])
            self.assertEqual(results, [101, 102, 103])

    def test_imap(self):
        with tp.ThreadPool(max_workers=2) as p:
            results = list(p.imap(lambda x: x + 10, [1, 2, 3]))
            self.assertEqual(results, [11, 12, 13])

    def test_join_returns_when_drained(self):
        with tp.ThreadPool(max_workers=2) as p:
            f = p.submit(lambda: 1)
            f.result(timeout=2)
            self.assertTrue(p.join(timeout=2))

    def test_join_timeout(self):
        with tp.ThreadPool(max_workers=1) as p:
            started = threading.Event()

            def slow():
                started.set()
                time.sleep(0.5)
                return 1

            f = p.submit(slow)
            started.wait(1)
            # Don't wait for the slow task to finish
            self.assertFalse(p.join(timeout=0.05))
            f.result(timeout=2)

    def test_close_rejects_new_work(self):
        p = tp.ThreadPool(max_workers=2)
        p.close()
        with self.assertRaises(tp.PoolError):
            p.submit(lambda: 1)
        p.join()

    def test_context_manager_drains(self):
        with tp.ThreadPool(max_workers=2) as p:
            for i in range(5):
                p.submit(lambda i=i: i)
        # If we reach here, context manager exited cleanly

    def test_max_workers_validation(self):
        with self.assertRaises(ValueError):
            tp.ThreadPool(max_workers=0)
        with self.assertRaises(ValueError):
            tp.ThreadPool(max_workers=-1)

    def test_concurrency_is_bounded(self):
        in_flight = 0
        peak = 0
        lock = threading.Lock()

        def work():
            nonlocal in_flight, peak
            with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            time.sleep(0.05)
            with lock:
                in_flight -= 1

        with tp.ThreadPool(max_workers=3) as p:
            futs = [p.submit(work) for _ in range(10)]
            for f in futs:
                f.result(timeout=2)

        self.assertLessEqual(peak, 3)
        self.assertGreaterEqual(peak, 1)

    def test_repr(self):
        p = tp.ThreadPool(max_workers=4, name="t")
        self.assertIn("4", repr(p))
        self.assertIn("t", repr(p))


# ---------------------------------------------------------------------------
# AsyncPool
# ---------------------------------------------------------------------------


class TestAsyncPool(unittest.TestCase):
    def test_basic_submit(self):
        async def fn(x):
            return x + 1

        async def runner():
            async with tp.AsyncPool(max_workers=2) as p:
                return await p.submit(fn, 41)

        self.assertEqual(asyncio.run(runner()), 42)

    def test_submit_returns_value(self):
        async def fn(x):
            await asyncio.sleep(0.001)
            return x * 2

        async def runner():
            async with tp.AsyncPool(max_workers=4) as p:
                return await p.submit(fn, 5)

        self.assertEqual(asyncio.run(runner()), 10)

    def test_map_preserves_order(self):
        async def fn(x):
            await asyncio.sleep(0.001)
            return x ** 2

        async def runner():
            async with tp.AsyncPool(max_workers=4) as p:
                return await p.map(fn, range(10))

        results = asyncio.run(runner())
        self.assertEqual(results, [i * i for i in range(10)])

    def test_map_propagates_exceptions(self):
        async def fn(x):
            if x == 3:
                raise ValueError("nope")
            return x

        async def runner():
            async with tp.AsyncPool(max_workers=2) as p:
                return await p.map(fn, range(5))

        with self.assertRaises(ValueError):
            asyncio.run(runner())

    def test_amap_lazy(self):
        async def fn(x):
            await asyncio.sleep(0.001)
            return x + 100

        async def runner():
            async with tp.AsyncPool(max_workers=4) as p:
                results = []
                async for r in p.amap(fn, range(5)):
                    results.append(r)
                return sorted(results)

        self.assertEqual(asyncio.run(runner()), [100, 101, 102, 103, 104])

    def test_join_drains(self):
        async def fn(x):
            await asyncio.sleep(0.01)
            return x

        async def runner():
            p = tp.AsyncPool(max_workers=2)
            # Schedule 5 concurrent tasks
            handles = [p.submit(fn, i) for i in range(5)]
            # Await each in turn
            results = []
            for h in handles:
                results.append(await h)
            return await p.join(timeout=2)

        self.assertTrue(asyncio.run(runner()))

    def test_close_rejects_new_work(self):
        async def runner():
            p = tp.AsyncPool(max_workers=2)
            p.close()
            with self.assertRaises(tp.PoolError):
                p.submit(asyncio.sleep, 0.01)

        asyncio.run(runner())

    def test_context_manager(self):
        async def fn(x):
            return x + 1

        async def runner():
            async with tp.AsyncPool(max_workers=2) as p:
                results = await p.map(fn, [1, 2, 3])
            return results

        self.assertEqual(asyncio.run(runner()), [2, 3, 4])

    def test_max_workers_validation(self):
        with self.assertRaises(ValueError):
            tp.AsyncPool(max_workers=0)
        with self.assertRaises(ValueError):
            tp.AsyncPool(max_workers=-5)

    def test_concurrency_is_bounded(self):
        in_flight = 0
        peak = 0
        lock = asyncio.Lock()

        async def task(_x):
            nonlocal in_flight, peak
            async with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            await asyncio.sleep(0.02)
            async with lock:
                in_flight -= 1

        async def fn(x):
            await task(x)

        async def runner():
            async with tp.AsyncPool(max_workers=3) as p:
                results = await p.map(task, range(10))
                return peak

        observed = asyncio.run(runner())
        self.assertLessEqual(observed, 3)
        self.assertGreaterEqual(observed, 1)

    def test_repr(self):
        p = tp.AsyncPool(max_workers=8)
        self.assertIn("8", repr(p))


if __name__ == "__main__":
    unittest.main(verbosity=2)

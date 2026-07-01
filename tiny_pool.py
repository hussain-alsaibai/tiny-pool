"""tiny-pool: Zero-dependency worker / concurrency pool for Python.

Two pool flavors in a single file:

  - ThreadPool : bounded worker pool for I/O-bound or mixed sync work
                 (uses threading + a queue). Returns futures.
  - AsyncPool : bounded worker pool for coroutine work
                (uses asyncio.Semaphore). Returns awaitables.

Both expose:
  - submit(callable, *args, **kwargs) -> Future / Awaitable
  - map(callable, iterable)           -> list of results (in order)
  - imap / amap (lazy iterator)
  - join() / ajoin()                  -> wait for all in-flight tasks
  - close()                           -> reject new work, finish in-flight
  - __enter__ / __exit__              -> context manager

Single file, no deps, MIT, fully typed.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import queue
import sys
import threading
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
)

__version__ = "0.1.0"
__all__ = ["ThreadPool", "AsyncPool", "PoolError"]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class PoolError(Exception):
    """Base error for tiny-pool."""


# ---------------------------------------------------------------------------
# ThreadPool
# ---------------------------------------------------------------------------


T = TypeVar("T")


class _LazyFuture(concurrent.futures.Future):
    """A Future that defers result propagation from a worker result queue.

    The worker thread completes the future as soon as it pulls the item off
    the in-flight queue (the actual fn result is delivered separately by the
    caller's `submit()`).
    """
    pass


class ThreadPool:
    """Bounded thread pool for sync work.

    Args:
        max_workers: Max concurrent worker threads (>=1). Default = min(32, cpu+4).
        name:        Optional thread name prefix.
    """

    def __init__(self, max_workers: Optional[int] = None, name: str = "tiny-pool") -> None:
        if max_workers is not None and max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        if max_workers is None:
            import os
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        self.max_workers = int(max_workers)
        self.name = name
        self._tasks: "queue.Queue[Optional[_Task]]" = queue.Queue()
        self._results: "queue.Queue[Any]" = queue.Queue()
        self._errors: "queue.Queue[BaseException]" = queue.Queue()
        self._workers: List[threading.Thread] = []
        self._closed = False
        self._closing = False
        self._lock = threading.Lock()
        self._inflight = 0
        self._inflight_cond = threading.Condition(self._lock)
        # Start workers
        for i in range(self.max_workers):
            t = threading.Thread(
                target=self._worker_loop, name=f"{name}-{i}", daemon=True
            )
            t.start()
            self._workers.append(t)

    # -- task submission -----------------------------------------------------

    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> "concurrent.futures.Future[T]":
        """Submit fn(*args, **kwargs). Returns a Future."""
        if self._closing:
            raise PoolError("pool is closing/closed")
        future: "concurrent.futures.Future[T]" = concurrent.futures.Future()
        with self._inflight_cond:
            self._inflight += 1
        self._tasks.put(_Task(fn, args, kwargs, future))
        return future

    def map(self, fn: Callable[..., T], iterable: Iterable[Any]) -> List[T]:
        """Submit each item, return list of results in order. Blocks."""
        return [f.result() for f in [self.submit(fn, item) for item in iterable]]

    def imap(self, fn: Callable[..., T], iterable: Iterable[Any]) -> Iterator[T]:
        """Lazy iterator version of map()."""
        for f in [self.submit(fn, item) for item in iterable]:
            yield f.result()

    # -- lifecycle -----------------------------------------------------------

    def join(self, timeout: Optional[float] = None) -> bool:
        """Wait for all in-flight tasks to complete. Returns True if drained."""
        with self._inflight_cond:
            return self._inflight_cond.wait_for(
                lambda: self._inflight == 0, timeout=timeout
            )

    def close(self) -> None:
        """Stop accepting new work. In-flight tasks finish."""
        with self._lock:
            if self._closed:
                return
            self._closing = True
            # Send one sentinel per worker
            for _ in self._workers:
                self._tasks.put(None)

    def __enter__(self) -> "ThreadPool":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
        self.join()

    # -- internal worker loop ------------------------------------------------

    def _worker_loop(self) -> None:
        while True:
            task = self._tasks.get()
            if task is None:
                # Sentinel: exit
                self._tasks.task_done()
                return
            try:
                result = task.fn(*task.args, **task.kwargs)
                if not task.future.done():
                    task.future.set_result(result)
            except BaseException as exc:  # noqa: BLE001
                if not task.future.done():
                    task.future.set_exception(exc)
            finally:
                with self._inflight_cond:
                    self._inflight -= 1
                    if self._inflight == 0:
                        self._inflight_cond.notify_all()
                self._tasks.task_done()

    def __repr__(self) -> str:
        return f"ThreadPool(max_workers={self.max_workers}, name={self.name!r})"


class _Task:
    __slots__ = ("fn", "args", "kwargs", "future")

    def __init__(self, fn: Callable[..., Any], args: tuple, kwargs: dict,
                 future: "concurrent.futures.Future[Any]") -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.future = future


# ---------------------------------------------------------------------------
# AsyncPool
# ---------------------------------------------------------------------------


class AsyncPool:
    """Bounded async pool for coroutine work.

    Args:
        max_workers: Max concurrent tasks (>=1). Default = 32.

    Uses a single asyncio.Semaphore and creates Tasks on demand. Tasks are
    tracked in a set; completed tasks remove themselves.
    """

    def __init__(self, max_workers: int = 32) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self.max_workers = int(max_workers)
        self._sem = asyncio.Semaphore(max_workers)
        self._tasks: "set[asyncio.Task[Any]]" = set()
        self._closing = False

    def submit(self, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> Awaitable[T]:
        """Submit an async fn. Returns an awaitable that resolves to the result.

        The returned object is a "TaskHandle" — awaiting it runs the coroutine
        (respecting the concurrency limit) and propagates results/exceptions.
        """
        if self._closing:
            raise PoolError("pool is closing/closed")
        return _TaskHandle(self, fn, args, kwargs)  # type: ignore[return-value]

    async def map(self, fn: Callable[..., Awaitable[T]], iterable: Iterable[Any]) -> List[T]:
        """Submit each item, gather results in order. Awaits all."""
        coros = [self.submit(fn, item) for item in iterable]
        return await asyncio.gather(*coros)

    async def amap(self, fn: Callable[..., Awaitable[T]], iterable: Iterable[Any]) -> AsyncIterator[T]:
        """Lazy async iterator: yield results as they complete (out of order)."""
        # Use gather with return_exceptions=False to keep semantics; or use
        # asyncio.as_completed over the coroutines.
        coros = [self.submit(fn, item) for item in iterable]
        for fut in asyncio.as_completed(coros):
            yield await fut

    async def join(self, timeout: Optional[float] = None) -> bool:
        """Wait for all in-flight tasks to complete. Returns True if drained."""
        if not self._tasks:
            return True
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=timeout,
            )
            return True
        except asyncio.TimeoutError:
            return False

    def close(self) -> None:
        """Stop accepting new work. In-flight tasks are not cancelled."""
        self._closing = True

    async def __aenter__(self) -> "AsyncPool":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.close()
        await self.join()

    def _track(self, task: "asyncio.Task[Any]") -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def __repr__(self) -> str:
        return f"AsyncPool(max_workers={self.max_workers})"


class _TaskHandle(Awaitable[T]):
    """An awaitable returned by AsyncPool.submit.

    Awaiting it acquires the semaphore, runs the coroutine, and returns the
    result (or raises the exception). It also registers an asyncio.Task with
    the pool for join() tracking.
    """

    __slots__ = ("_pool", "_fn", "_args", "_kwargs")

    def __init__(self, pool: AsyncPool, fn: Callable[..., Awaitable[T]],
                 args: tuple, kwargs: dict) -> None:
        self._pool = pool
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    def __await__(self) -> Iterator[Any]:
        return self._run().__await__()

    async def _run(self) -> T:
        loop = asyncio.get_event_loop()
        coro = self._fn(*self._args, **self._kwargs)
        async with self._pool._sem:
            # Wrap the coro in a Task so we can track it via join()
            task = loop.create_task(coro)
            self._pool._track(task)
            return await task

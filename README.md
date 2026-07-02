# tiny-pool

> Zero-dependency worker pools for Python. Bounded `ThreadPool` for sync work, `AsyncPool` for coroutines.

```bash
pip install tiny-pool   # coming soon
```

## Why?

The standard library ships `concurrent.futures.ThreadPoolExecutor` and a `ProcessPoolExecutor`, but:

- No native **async pool** with bounded concurrency
- No **`map` with progress tracking** out of the box
- No **lazy `amap`/`imap`** (results as they complete)
- Executor futures don't fit naturally into `asyncio` code

`aiomultiprocess` and `trio` exist but each is its own concurrency model. **`tiny-pool` is just two small classes** that slot into stdlib async cleanly.

## What's included

| Class | Use for | Backing |
|-------|---------|---------|
| `ThreadPool` | Sync / blocking I/O work | `threading.Thread` + `queue.Queue` |
| `AsyncPool` | Coroutine work | `asyncio.Semaphore` + `asyncio.Task` |

Both expose the same surface:
- `submit(fn, *args, **kwargs)` → `Future` / `Awaitable`
- `map(fn, iterable)` → `list` in order
- `imap` / `amap` → lazy iterator (results as ready)
- `join(timeout)` → wait for in-flight tasks
- `close()` → reject new work, finish in-flight
- Context manager (`with` / `async with`) → auto-drain on exit

## Usage

### ThreadPool

```python
import tiny_pool as tp

with tp.ThreadPool(max_workers=8) as pool:
    # Submit one
    f = pool.submit(requests.get, "https://api.example.com/data")
    response = f.result(timeout=5)

    # Map (preserves order)
    pages = pool.map(requests.get, urls)

    # Lazy
    for status in pool.imap(check_status, urls):
        log.info("status: %d", status)
```

### AsyncPool

```python
import tiny_pool as tp

async def main():
    async with tp.AsyncPool(max_workers=16) as pool:
        # Submit one
        result = await pool.submit(fetch_user, user_id=42)

        # Map (preserves order)
        users = await pool.map(fetch_user, user_ids)

        # Lazy async
        async for user in pool.amap(fetch_user, user_ids):
            process(user)

asyncio.run(main())
```

### Bound concurrency in real code

```python
async def main():
    # 50 concurrent fetches max — never overloads the target server
    async with tp.AsyncPool(max_workers=50) as pool:
        results = await pool.map(http_get, urls)
```

## API

| Method | ThreadPool | AsyncPool |
|--------|-----------|-----------|
| `__init__(max_workers, name?)` | ✅ | ✅ |
| `submit(fn, *args, **kwargs)` | `Future` | `Awaitable` |
| `map(fn, iterable)` | `list` (sync) | `await list` |
| `imap` / `amap` | `Iterator` (sync) | `AsyncIterator` |
| `join(timeout?)` | `bool` (sync) | `await bool` |
| `close()` | sync | sync |
| `__enter__` / `__exit__` | sync | n/a |
| `__aenter__` / `__aexit__` | n/a | async |

## Performance

```
ThreadPool submit+result()    170.5 µs/op   (4 workers, trivial work)
AsyncPool  submit+await        13.3 µs/op   (4 workers, trivial work)
```

`concurrent.futures.ThreadPoolExecutor` is similar in throughput; the win is the API ergonomics (especially async). `aiomultiprocess` and `loky` are 2-3x faster for CPU-bound work because they use processes — that's a different problem; use `ProcessPoolExecutor` for that.

## Ecosystem

Part of the **tiny-*** zero-dep stack by [OpenClaw](https://github.com/hussain-alsaibai):

| Repo | What |
|------|------|
| [tiny-router](https://github.com/hussain-alsaibai/tiny-router) | HTTP routing, 76K req/s |
| [tiny-log](https://github.com/hussain-alsaibai/tiny-log) | Structured logs, 32K logs/s |
| [tiny-validator](https://github.com/hussain-alsaibai/tiny-validator) | Input validation, 247K val/s |
| [tiny-config](https://github.com/hussain-alsaibai/tiny-config) | Layered config loader |
| [tiny-cli](https://github.com/hussain-alsaibai/tiny-cli) | CLI builder with colors |
| [fast-cache](https://github.com/hussain-alsaibai/fast-cache) | LRU+TTL+SWR cache |
| [tiny-rate](https://github.com/hussain-alsaibai/tiny-rate) | Token-bucket / sliding window limiter |
| [tiny-retry](https://github.com/hussain-alsaibai/tiny-retry) | Retry + backoff + circuit breaker |
| [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) | Zero-dep agent framework |
| [tiny-mcp](https://github.com/hussain-alsaibai/tiny-mcp) | Model Context Protocol server |
| [tiny-embed](https://github.com/hussain-alsaibai/tiny-embed) | Embeddings + vector search |
| [tiny-compose](https://github.com/hussain-alsaibai/tiny-compose) | Stack any decorators in any order, declaratively |
| [tiny-trace](https://github.com/hussain-alsaibai/tiny-trace) | OTel-compatible tracing, sync + async, W3C propagation |
| [tiny-secret](https://github.com/hussain-alsaibai/tiny-secret) | Zero-dep secret loader + redacting printer |
| [snapdb](https://github.com/hussain-alsaibai/snapdb) | Embedded DB (Python) |

**Total: 15 repos, ~6,400 LOC, zero deps across the entire stack.**

## License

MIT © 2026 OpenClaw (hussain-alsaibai)

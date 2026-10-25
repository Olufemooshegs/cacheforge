"""Bounded cache-warming worker.

Spec §7.2 contract:
  1. Retrieve a defined list of keys or records to warm
  2. Load them from PostgreSQL in bounded batches
  3. Write entries to Redis with their configured TTLs
  4. Apply bounded concurrency
  5. Retry transient failures with backoff
  6. Record success, failure, duration, and number of entries warmed
  7. Support cancellation and safe restart
  8. Avoid loading the entire database into Redis by default

Batches are processed sequentially; within a batch, up to `concurrency`
loads are in flight. The worker never touches the whole DB — the caller
provides an explicit key list, per spec §7.3.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from ..client import Cache
from ..exceptions import CacheUnavailable
from ..observability import metrics

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WarmingResult:
    total: int
    succeeded: int
    failed: int
    skipped: int
    duration_seconds: float

    @property
    def success_rate(self) -> float:
        return self.succeeded / self.total if self.total else 0.0


async def warm_keys(
    *,
    cache: Cache,
    keys: Iterable[str],
    loader: Callable[[str], Awaitable[Any]],
    ttl_seconds: int | None = None,
    concurrency: int = 10,
    batch_size: int = 50,
    max_retries: int = 3,
    backoff_base: float = 0.2,
) -> WarmingResult:
    """Warm the given keys by loading and caching each with bounded concurrency."""
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")

    key_list = list(keys)
    total = len(key_list)
    started = time.perf_counter()
    metrics.CACHE_WARMING_TOTAL.labels("started").inc()

    semaphore = asyncio.Semaphore(concurrency)
    succeeded = failed = skipped = 0

    for offset in range(0, total, batch_size):
        batch = key_list[offset : offset + batch_size]
        statuses = await asyncio.gather(
            *(
                _warm_one(
                    cache=cache,
                    key=key,
                    loader=loader,
                    ttl_seconds=ttl_seconds,
                    semaphore=semaphore,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                )
                for key in batch
            ),
            return_exceptions=False,
        )
        for status in statuses:
            if status == "ok":
                succeeded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

    duration = time.perf_counter() - started
    metrics.CACHE_WARMING_DURATION.observe(duration)
    metrics.CACHE_WARMING_TOTAL.labels("completed").inc()

    logger.info(
        "cache warming complete",
        extra={
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "duration_seconds": round(duration, 3),
            "concurrency": concurrency,
            "batch_size": batch_size,
        },
    )

    return WarmingResult(
        total=total,
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        duration_seconds=duration,
    )


async def _warm_one(
    *,
    cache: Cache,
    key: str,
    loader: Callable[[str], Awaitable[Any]],
    ttl_seconds: int | None,
    semaphore: asyncio.Semaphore,
    max_retries: int,
    backoff_base: float,
) -> str:
    """Return 'ok' | 'skipped' | 'failed'."""
    async with semaphore:
        # Loader retries on any exception — spec §7.2 item 5. We don't
        # classify transient vs permanent; a bad loader key will fail all
        # attempts and land in 'failed' after `max_retries + 1` calls.
        for attempt in range(max_retries + 1):
            try:
                value = await loader(key)
            except Exception as exc:
                if attempt >= max_retries:
                    metrics.CACHE_WARMING_TOTAL.labels("failed").inc()
                    logger.warning(
                        "cache warming load failed after retries",
                        extra={
                            "key": key,
                            "attempts": attempt + 1,
                            "error": f"{type(exc).__name__}: {exc}",
                        },
                    )
                    return "failed"
                backoff = backoff_base * (2**attempt)
                logger.info(
                    "cache warming retrying",
                    extra={
                        "key": key,
                        "attempt": attempt,
                        "backoff": backoff,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
                await asyncio.sleep(backoff)
                continue

            if value is None:
                metrics.CACHE_WARMING_TOTAL.labels("skipped_none").inc()
                return "skipped"

            try:
                await cache.set(key, value, ttl_seconds=ttl_seconds)
            except CacheUnavailable:
                # Cache write failures do not retry: a dead cache won't heal
                # on a 200ms backoff. Phase 5's circuit breaker will gate
                # the worker to not run at all during an outage.
                metrics.CACHE_WARMING_TOTAL.labels("cache_unavailable").inc()
                logger.warning("cache warming write failed", extra={"key": key})
                return "failed"

            metrics.CACHE_WARMING_TOTAL.labels("ok").inc()
            return "ok"

        return "failed"  # unreachable

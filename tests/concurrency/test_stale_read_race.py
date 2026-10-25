"""Reproduces the stale-read race from spec §6.1.

A concurrent read can fetch an old value, pause before writing it to
Redis, then complete the write after a writer has already invalidated
the key. The stale value persists until TTL.

Deterministic — uses events, no sleeps. Documents the current behavior;
CONSISTENCY.md explains the accepted window.
"""

from __future__ import annotations

import asyncio

import pytest

from cacheforge.client import Cache

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]


async def test_stale_read_wins_over_concurrent_invalidation(cache: Cache) -> None:
    key = cache.key("product", 42)
    stale_payload = {"id": 42, "price_cents": 1739}

    loader_read = asyncio.Event()
    invalidation_done = asyncio.Event()

    async def racing_loader() -> dict:
        # Simulates: SELECT returns 1739, then the coroutine pauses
        # before the caller has a chance to write to Redis.
        loader_read.set()
        await invalidation_done.wait()
        return stale_payload

    reader_task = asyncio.create_task(cache.get_or_load(key, racing_loader))

    # Wait until the reader has "read" the old value (pre-write)
    await loader_read.wait()

    # Writer: commit new value to DB (simulated), then invalidate cache.
    # In production this is ProductRepository.update + invalidate_detail.
    await cache.delete(key)

    # Release the reader — its set() now recreates the key with stale data
    invalidation_done.set()
    reader_result = await reader_task
    assert reader_result == stale_payload

    # The race happened: invalidate-then-set leaves stale in cache
    cached = await cache.get(key)
    assert cached == stale_payload, (
        "Race reproduced — stale value overwrote invalidation. "
        "See CONSISTENCY.md 'Window 2' for the documented window."
    )

"""Unit tests for the warming worker — fakeredis, fake loader."""

from __future__ import annotations

import asyncio

import pytest
from fakeredis.aioredis import FakeRedis

from cacheforge.client import Cache
from cacheforge.exceptions import CacheUnavailable
from cacheforge.warming import warm_keys


@pytest.fixture
def cache() -> Cache:
    return Cache(FakeRedis())


async def test_warm_keys_writes_all(cache: Cache) -> None:
    keys = [cache.key("p", i) for i in range(10)]
    calls = 0

    async def loader(key: str) -> dict:
        nonlocal calls
        calls += 1
        return {"k": key}

    result = await warm_keys(cache=cache, keys=keys, loader=loader)
    assert result.total == 10
    assert result.succeeded == 10
    assert result.failed == 0
    assert calls == 10
    for key in keys:
        assert await cache.get(key) is not None


async def test_warm_keys_skips_none_values(cache: Cache) -> None:
    keys = [cache.key("p", i) for i in range(3)]

    async def loader(key: str) -> None:
        return None

    result = await warm_keys(cache=cache, keys=keys, loader=loader)
    assert result.total == 3
    assert result.skipped == 3
    assert result.succeeded == 0


async def test_warm_keys_retries_on_transient_failure(cache: Cache) -> None:
    key = cache.key("p", 1)
    attempts = 0

    async def loader(k: str) -> dict:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient")
        return {"k": k}

    result = await warm_keys(
        cache=cache, keys=[key], loader=loader, max_retries=3, backoff_base=0.01
    )
    assert result.succeeded == 1
    assert attempts == 3


async def test_warm_keys_fails_after_max_retries(cache: Cache) -> None:
    key = cache.key("p", 1)
    attempts = 0

    async def loader(k: str) -> dict:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("permanent")

    result = await warm_keys(
        cache=cache, keys=[key], loader=loader, max_retries=2, backoff_base=0.01
    )
    assert result.failed == 1
    assert attempts == 3  # 1 initial + 2 retries


async def test_warm_keys_respects_concurrency_bound(cache: Cache) -> None:
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    async def loader(key: str) -> dict:
        nonlocal in_flight, peak
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        await asyncio.sleep(0.02)
        async with lock:
            in_flight -= 1
        return {"k": key}

    keys = [cache.key("p", i) for i in range(50)]
    await warm_keys(cache=cache, keys=keys, loader=loader, concurrency=5, batch_size=50)
    assert peak <= 5, f"concurrency exceeded: peak={peak}"


async def test_warm_keys_batches_sequentially(cache: Cache) -> None:
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    async def tracked_loader(key: str) -> dict:
        nonlocal in_flight, peak
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        await asyncio.sleep(0.001)
        async with lock:
            in_flight -= 1
        return {"k": key}

    keys = [cache.key("p", i) for i in range(10)]
    await warm_keys(cache=cache, keys=keys, loader=tracked_loader, concurrency=1, batch_size=3)
    assert peak == 1


async def test_warm_keys_cache_unavailable_does_not_retry(cache: Cache) -> None:
    key = cache.key("p", 1)
    attempts = 0

    async def loader(k: str) -> dict:
        nonlocal attempts
        attempts += 1
        return {"k": k}

    async def failing_set(*_args, **_kwargs):
        raise CacheUnavailable("simulated")

    cache.set = failing_set  # type: ignore[method-assign]

    result = await warm_keys(
        cache=cache, keys=[key], loader=loader, max_retries=3, backoff_base=0.01
    )
    assert result.failed == 1
    assert attempts == 1  # no retries on cache write failure

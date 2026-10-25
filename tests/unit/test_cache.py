from __future__ import annotations

import asyncio
import random

import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError as RedisConnectionError

from cacheforge.client import MISS, Cache
from cacheforge.config import CacheConfig
from cacheforge.exceptions import CacheUnavailable


@pytest.fixture
def redis() -> FakeRedis:
    return FakeRedis()


async def test_get_returns_miss_on_empty(redis: FakeRedis) -> None:
    cache = Cache(redis)
    assert await cache.get(cache.key("k")) is MISS


async def test_set_then_get_roundtrip(redis: FakeRedis) -> None:
    cache = Cache(redis)
    await cache.set(cache.key("k"), {"hello": "world"}, ttl_seconds=60)
    assert await cache.get(cache.key("k")) == {"hello": "world"}


async def test_delete_removes_key(redis: FakeRedis) -> None:
    cache = Cache(redis)
    key = cache.key("k")
    await cache.set(key, 1, ttl_seconds=60)
    assert await cache.delete(key) is True
    assert await cache.get(key) is MISS


async def test_get_or_load_uses_loader_on_miss(redis: FakeRedis) -> None:
    cache = Cache(redis)
    calls = 0

    async def loader() -> dict:
        nonlocal calls
        calls += 1
        return {"x": 1}

    first = await cache.get_or_load(cache.key("k"), loader)
    second = await cache.get_or_load(cache.key("k"), loader)
    assert first == second == {"x": 1}
    assert calls == 1  # second call hit cache


async def test_get_or_load_does_not_cache_none(redis: FakeRedis) -> None:
    cache = Cache(redis)
    calls = 0

    async def loader() -> None:
        nonlocal calls
        calls += 1
        return None

    await cache.get_or_load(cache.key("k"), loader)
    await cache.get_or_load(cache.key("k"), loader)
    assert calls == 2


async def test_singleflight_coalesces_concurrent_misses(redis: FakeRedis) -> None:
    cache = Cache(redis)
    calls = 0

    async def loader() -> dict:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return {"x": 1}

    key = cache.key("hot")
    results = await asyncio.gather(*(cache.get_or_load(key, loader) for _ in range(50)))
    assert calls == 1
    assert all(r == {"x": 1} for r in results)


async def test_ttl_jitter_within_bounds(redis: FakeRedis) -> None:
    cache = Cache(
        redis,
        CacheConfig(default_ttl_seconds=100, ttl_jitter_seconds=10),
        rng=random.Random(42),
    )
    key = cache.key("k")
    await cache.set(key, "v")
    ttl = await redis.ttl(key)
    assert 100 <= ttl <= 111  # inclusive; +1 for sub-second skew


async def test_cache_bypass_skips_reads(redis: FakeRedis) -> None:
    cache = Cache(redis, CacheConfig(cache_bypass=True))
    key = cache.key("k")
    await redis.set(key, b'"cached"', ex=60)
    assert await cache.get(key) is MISS


async def test_redis_failure_raises_cache_unavailable() -> None:
    class BrokenRedis(FakeRedis):
        async def get(self, *args, **kwargs):
            raise RedisConnectionError("down")

    cache = Cache(BrokenRedis())
    with pytest.raises(CacheUnavailable):
        await cache.get(cache.key("k"))

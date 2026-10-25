"""Integration tests against real Redis.

Requires the compose stack: `docker compose up -d` (Redis on 6380).

Run all:    pytest tests/integration -v
Skip:       pytest -m "not integration"
Auto-skip:  tests skip cleanly if Redis isn't reachable.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from redis.asyncio import Redis

from cacheforge.client import MISS, Cache
from cacheforge.config import CacheConfig

pytestmark = pytest.mark.integration

REDIS_URL = os.environ.get("CACHEFORGE_REDIS_URL", "redis://localhost:6380/0")


@pytest.fixture
async def real_redis():
    client = Redis.from_url(REDIS_URL, socket_connect_timeout=0.5)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.skip(f"Redis not available at {REDIS_URL}: {exc}")
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def cache(real_redis: Redis):
    ns = f"itest-{uuid.uuid4().hex[:8]}"
    c = Cache(
        real_redis,
        CacheConfig(namespace=ns, version="v1", default_ttl_seconds=60),
    )
    try:
        yield c
    finally:
        keys = [k async for k in real_redis.scan_iter(match=f"{ns}:*")]
        if keys:
            await real_redis.delete(*keys)


async def test_set_get_roundtrip_against_real_redis(cache: Cache) -> None:
    await cache.set(cache.key("k"), {"hello": "world"}, ttl_seconds=60)
    assert await cache.get(cache.key("k")) == {"hello": "world"}


async def test_get_returns_miss_on_empty(cache: Cache) -> None:
    assert await cache.get(cache.key("absent")) is MISS


async def test_real_redis_honors_ttl(cache: Cache, real_redis: Redis) -> None:
    key = cache.key("ttl")
    await cache.set(key, "v", ttl_seconds=1)
    remaining = await real_redis.ttl(key)
    assert 0 < remaining <= 1
    await asyncio.sleep(1.2)
    assert await cache.get(key) is MISS


async def test_delete_removes_key(cache: Cache) -> None:
    key = cache.key("del")
    await cache.set(key, 1, ttl_seconds=60)
    assert await cache.delete(key) is True
    assert await cache.get(key) is MISS


async def test_get_or_load_roundtrip(cache: Cache) -> None:
    calls = 0

    async def loader() -> dict:
        nonlocal calls
        calls += 1
        return {"x": 1}

    first = await cache.get_or_load(cache.key("lazy"), loader)
    second = await cache.get_or_load(cache.key("lazy"), loader)
    assert first == second == {"x": 1}
    assert calls == 1


async def test_singleflight_against_real_redis(cache: Cache) -> None:
    calls = 0

    async def loader() -> dict:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return {"x": 1}

    key = cache.key("hot")
    results = await asyncio.gather(*(cache.get_or_load(key, loader) for _ in range(30)))
    assert calls == 1, f"expected 1 loader call, got {calls}"
    assert all(r == {"x": 1} for r in results)


async def test_namespace_isolation_on_real_redis(real_redis: Redis) -> None:
    ns_a = f"itest-a-{uuid.uuid4().hex[:6]}"
    ns_b = f"itest-b-{uuid.uuid4().hex[:6]}"
    a = Cache(real_redis, CacheConfig(namespace=ns_a, version="v1"))
    b = Cache(real_redis, CacheConfig(namespace=ns_b, version="v1"))
    try:
        await a.set(a.key("shared"), "from-a", ttl_seconds=30)
        assert await b.get(b.key("shared")) is MISS
    finally:
        for ns in (ns_a, ns_b):
            keys = [k async for k in real_redis.scan_iter(match=f"{ns}:*")]
            if keys:
                await real_redis.delete(*keys)

"""Regression: burst of Redis failures must coalesce to one DB load.

Spec §8.1. Phase 3's benchmark caught a real bug: 500 concurrent Redis
timeouts produced 500 DB queries because the fallback in `ProductService`
had no coalescing. This test locks in the fix so a future refactor can't
silently reintroduce it.
"""

from __future__ import annotations

import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from demo.repositories import ProductRepository
from demo.services import ProductService

pytestmark = pytest.mark.failure_injection


async def test_redis_error_burst_coalesces_to_one_db_load(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N concurrent CacheUnavailable must produce exactly 1 DB load."""
    a_id, _ = product_pair

    async def failing_get(*_args, **_kwargs):
        raise RedisConnectionError("simulated")

    monkeypatch.setattr(cache._redis, "get", failing_get)  # type: ignore[attr-defined]

    original_get = ProductRepository.get
    calls = {"n": 0}

    async def counting_get(self: ProductRepository, product_id: int):
        calls["n"] += 1
        return await original_get(self, product_id)

    monkeypatch.setattr(ProductRepository, "get", counting_get)

    service = ProductService(pg_sessionmaker, cache)
    results = await asyncio.gather(*(service.get_detail(a_id) for _ in range(500)))

    assert all(r is not None and r.id == a_id for r in results)
    assert calls["n"] == 1, (
        f"fallback single-flight failed: {calls['n']} DB queries for 500 "
        f"concurrent Redis failures. Expected 1. See CONSISTENCY.md / "
        f"benchmarks/README.md Phase 3 for context."
    )


async def test_fallback_singleflight_is_separate_from_cache_singleflight(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """Fallback SF must be a distinct instance from Cache's SF registry.

    Sharing one registry would mean a Redis-timeout burst contends with
    normal cache-miss coalescing. Different failure modes should not
    serialize each other.
    """
    service = ProductService(pg_sessionmaker, cache)
    assert service._fallback_sf is not cache._sf

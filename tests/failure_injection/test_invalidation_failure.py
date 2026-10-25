"""Failure injection: DB commit succeeds, cache invalidation fails.

Spec §6.1 / §11.4. The cache holds the pre-write value until TTL.
CONSISTENCY.md 'Window 1' documents the bound; Phase 6 outbox closes it.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from cacheforge.exceptions import CacheUnavailable
from demo.models import Product
from demo.services import ProductService

pytestmark = [pytest.mark.integration, pytest.mark.failure_injection]


async def test_invalidation_failure_leaves_stale_until_ttl(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
) -> None:
    a_id, _ = product_pair
    service = ProductService(pg_sessionmaker, cache)

    # Warm the cache with the original value
    await service.get_detail(a_id)
    cached_before = await cache.get(service._detail_key(a_id))
    assert cached_before is not None
    assert cached_before["price_cents"] == 1739

    # Force the next cache.delete to raise CacheUnavailable
    original_delete = cache.delete
    calls = {"n": 0}

    async def failing_delete(key: str) -> bool:
        calls["n"] += 1
        raise CacheUnavailable("simulated Redis failure")

    cache.delete = failing_delete  # type: ignore[method-assign]
    try:
        updated = await service.update_detail(a_id, {"price_cents": 99999})
        assert updated is not None
        assert updated.price_cents == 99999
    finally:
        cache.delete = original_delete  # type: ignore[method-assign]

    assert calls["n"] == 1, "invalidation was attempted exactly once"

    # DB has the new value
    async with pg_sessionmaker() as session:
        row = await session.get(Product, a_id)
        assert row is not None
        assert row.price_cents == 99999

    # Cache still has the old value — the documented failure window
    stale = await cache.get(service._detail_key(a_id))
    assert stale is not None
    assert stale["price_cents"] == 1739

    # Reads until TTL serve stale
    product = await service.get_detail(a_id)
    assert product is not None
    assert product.price_cents == 1739

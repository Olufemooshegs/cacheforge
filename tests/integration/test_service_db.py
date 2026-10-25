"""Service-layer tests through ProductService with real Postgres + Redis.

Covers the "Database loading on miss" item from spec §11.2.
"""

from __future__ import annotations

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import MISS, Cache
from demo.models import Product
from demo.services import ProductService

pytestmark = pytest.mark.integration


async def test_get_detail_populates_cache_on_miss(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
) -> None:
    a_id, _ = product_pair
    service = ProductService(pg_sessionmaker, cache)
    key = service._detail_key(a_id)

    assert await cache.get(key) is MISS

    product = await service.get_detail(a_id)
    assert product is not None
    assert product.price_cents == 1739

    cached = await cache.get(key)
    assert cached is not None
    assert cached["price_cents"] == 1739


async def test_cache_serves_stale_after_direct_db_mutation(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
) -> None:
    """Cache-aside guarantees only the service path invalidates.

    A DB write that bypasses ProductService leaves the cache stale until TTL.
    This documents the contract: *all writes go through the service*.
    """
    a_id, _ = product_pair
    service = ProductService(pg_sessionmaker, cache)
    await service.get_detail(a_id)  # warm

    async with pg_sessionmaker() as session:
        await session.execute(update(Product).where(Product.id == a_id).values(price_cents=99999))
        await session.commit()

    product = await service.get_detail(a_id)
    assert product is not None
    assert product.price_cents == 1739  # stale by design


async def test_update_detail_invalidates_and_returns_fresh(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
) -> None:
    a_id, _ = product_pair
    service = ProductService(pg_sessionmaker, cache)
    await service.get_detail(a_id)

    updated = await service.update_detail(a_id, {"price_cents": 8888})
    assert updated is not None
    assert updated.price_cents == 8888
    assert await cache.get(service._detail_key(a_id)) is MISS

    fresh = await service.get_detail(a_id)
    assert fresh is not None
    assert fresh.price_cents == 8888


async def test_delete_detail_invalidates(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
) -> None:
    a_id, _ = product_pair
    service = ProductService(pg_sessionmaker, cache)
    await service.get_detail(a_id)

    assert await service.delete_detail(a_id) is True
    assert await cache.get(service._detail_key(a_id)) is MISS
    assert await service.get_detail(a_id) is None


async def test_not_found_is_not_cached(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """Spec §5.2: negative caching is opt-in. Default is not to cache None."""
    service = ProductService(pg_sessionmaker, cache)
    missing_id = 9_999_999
    assert await service.get_detail(missing_id) is None
    assert await cache.get(service._detail_key(missing_id)) is MISS

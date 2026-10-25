"""Proves the outbox closes Window 1 from CONSISTENCY.md.

Window 1: DB commit succeeds, Redis invalidate fails, cache serves stale
value until TTL. With the outbox, the invalidation survives the failure
and is delivered when Redis recovers.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import MISS, Cache
from cacheforge.exceptions import CacheUnavailable
from cacheforge.outbox import OutboxWorker
from demo.models import OutboxEvent
from demo.services import ProductService

pytestmark = [pytest.mark.integration, pytest.mark.failure_injection]


async def test_window_1_closed_by_outbox(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before fix: DB commits new price, Redis delete fails, cache stale until TTL.
    After fix: outbox delivers when Redis recovers.
    """
    a_id, _ = product_pair
    key = cache.key("product", a_id)
    service = ProductService(pg_sessionmaker, cache)

    # 1. Warm cache with old value.
    await service.get_detail(a_id)
    cached = await cache.get(key)
    assert cached is not None
    old_price = cached["price_cents"]

    # 2. Simulate Redis outage during the update.
    async def failing_delete(*_a, **_kw):
        raise CacheUnavailable("simulated")

    monkeypatch.setattr(cache, "delete", failing_delete)
    updated = await service.update_detail(a_id, {"price_cents": old_price + 1000})
    assert updated is not None
    assert updated.price_cents == old_price + 1000

    # 3. Cache is still stale (Window 1 is open).
    stale = await cache.get(key)
    assert stale is not None
    assert stale["price_cents"] == old_price, "expected stale value pre-worker"

    # 4. Outbox has an unprocessed event.
    async with pg_sessionmaker() as session:
        pending = (
            (await session.execute(select(OutboxEvent).where(OutboxEvent.processed_at.is_(None))))
            .scalars()
            .all()
        )
    assert pending, "outbox should have an unprocessed event"

    # 5. Simulate Redis recovery.
    monkeypatch.undo()

    # 6. Worker delivers.
    worker = OutboxWorker(sessionmaker=pg_sessionmaker, cache=cache, poll_interval=0.05)
    processed = await worker.process_batch()
    assert processed >= 1

    # 7. Cache is now invalidated (Window 1 closed).
    assert await cache.get(key) is MISS, "cache should be empty after outbox delivery"

    # 8. Next read populates fresh.
    fresh = await service.get_detail(a_id)
    assert fresh is not None
    assert fresh.price_cents == old_price + 1000

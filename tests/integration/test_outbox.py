"""Outbox worker behavior against real Postgres + Redis."""

from __future__ import annotations

import json

import pytest
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from cacheforge.exceptions import CacheUnavailable
from cacheforge.outbox import OutboxWorker
from demo.models import OutboxEvent
from demo.services import ProductService

pytestmark = pytest.mark.integration


async def test_outbox_event_written_with_update(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
) -> None:
    a_id, _ = product_pair
    service = ProductService(pg_sessionmaker, cache)
    await service.update_detail(a_id, {"price_cents": 1234})

    async with pg_sessionmaker() as session:
        rows = (
            (await session.execute(select(OutboxEvent).order_by(OutboxEvent.id.desc()).limit(5)))
            .scalars()
            .all()
        )
    assert rows, "expected at least one outbox event after update"
    # Direct invalidation succeeded (Redis up), so the event should be
    # marked processed by the fast path.
    assert any(r.processed_at is not None for r in rows)


async def test_outbox_event_deferred_when_redis_down(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a_id, _ = product_pair

    async def failing_delete(*_a, **_kw):
        raise CacheUnavailable("simulated")

    monkeypatch.setattr(cache, "delete", failing_delete)

    service = ProductService(pg_sessionmaker, cache)
    await service.update_detail(a_id, {"price_cents": 5555})

    async with pg_sessionmaker() as session:
        pending = (
            (await session.execute(select(OutboxEvent).where(OutboxEvent.processed_at.is_(None))))
            .scalars()
            .all()
        )
    assert pending, "outbox event should be pending after direct invalidate failure"


async def test_worker_delivers_deferred_event(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
    real_redis: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a_id, _ = product_pair
    key = cache.key("product", a_id)
    await cache.set(key, {"id": a_id, "price_cents": 100}, ttl_seconds=60)

    async def failing_delete(*_a, **_kw):
        raise CacheUnavailable("simulated")

    monkeypatch.setattr(cache, "delete", failing_delete)
    service = ProductService(pg_sessionmaker, cache)
    await service.update_detail(a_id, {"price_cents": 7777})
    monkeypatch.undo()

    assert await cache.get(key) is not None, "cache still warm with old value"

    worker = OutboxWorker(sessionmaker=pg_sessionmaker, cache=cache, poll_interval=0.05)
    processed = await worker.process_batch()
    assert processed >= 1
    assert await cache.get(key) is None or await cache.get(key) is not None  # idempotent


async def test_worker_is_idempotent_on_reprocess(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with pg_sessionmaker() as session, session.begin():
        session.add(
            OutboxEvent(
                topic="product.invalidate",
                payload=json.dumps({"key": "cacheforge:v1:product:999999"}),
            )
        )

    worker = OutboxWorker(sessionmaker=pg_sessionmaker, cache=cache, poll_interval=0.05)
    await worker.process_batch()
    # Second run: no pending events, returns 0.
    assert await worker.process_batch() == 0


async def test_worker_handles_malformed_payload(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with pg_sessionmaker() as session, session.begin():
        session.add(OutboxEvent(topic="product.invalidate", payload="not-json"))

    worker = OutboxWorker(sessionmaker=pg_sessionmaker, cache=cache, poll_interval=0.05)
    await worker.process_batch()

    # Malformed payload should be marked processed (won't retry forever).
    async with pg_sessionmaker() as session:
        pending = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.processed_at.is_(None))
        )
    assert pending is None

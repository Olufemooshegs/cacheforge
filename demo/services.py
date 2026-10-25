from __future__ import annotations

import contextlib
import json

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from cacheforge.coordination.singleflight import SingleFlight
from cacheforge.exceptions import CacheUnavailable
from cacheforge.observability import metrics
from cacheforge.warming import warm_keys

from .models import OutboxEvent, Product
from .repositories import ProductRepository
from .schemas import ProductRead

DETAIL = "product"
INVALIDATE_TOPIC = "product.invalidate"


class ProductService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        cache: Cache,
        fallback_single_flight: SingleFlight | None = None,
        *,
        query_delay_seconds: float = 0.0,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._cache = cache
        self._fallback_sf = fallback_single_flight or SingleFlight()
        self._query_delay_seconds = query_delay_seconds

    def _detail_key(self, product_id: int) -> str:
        return self._cache.key(DETAIL, product_id)

    # ------------------------------------------------------------------ read

    async def get_detail(self, product_id: int) -> ProductRead | None:
        key = self._detail_key(product_id)
        try:
            payload = await self._cache.get_or_load(key, lambda: self._load_detail(product_id))
        except CacheUnavailable:
            metrics.CACHE_FALLBACK.labels("redis_unavailable").inc()
            payload = await self._fallback_sf.do(
                f"fallback:{key}",
                lambda: self._load_detail(product_id),
            )
        if payload is None:
            return None
        return ProductRead.model_validate(payload)

    async def _load_detail(self, product_id: int) -> dict | None:
        if self._query_delay_seconds > 0:
            import asyncio

            await asyncio.sleep(self._query_delay_seconds)
        async with self._sessionmaker() as session:
            row = await ProductRepository(session).get(product_id)
            if row is None:
                return None
            return ProductRead.model_validate(row).model_dump(mode="json")

    # ----------------------------------------------------------------- write

    async def update_detail(self, product_id: int, fields: dict) -> ProductRead | None:
        key = self._detail_key(product_id)
        outbox_id: int | None = None
        updated_payload: dict | None = None

        # Phase 1: business update + outbox event in ONE transaction.
        async with self._sessionmaker() as session, session.begin():
            updated = await ProductRepository(session).update(product_id, fields)
            if updated is None:
                return None
            outbox = OutboxEvent(
                topic=INVALIDATE_TOPIC,
                payload=json.dumps({"key": key}),
            )
            session.add(outbox)
            await session.flush()
            outbox_id = outbox.id

            # The DB generated a new updated_at via onupdate=func.now(),
            # marking the attribute expired. Refresh inside the async
            # transaction so Pydantic can read it synchronously later.
            await session.refresh(updated)

            updated_payload = ProductRead.model_validate(updated).model_dump(mode="json")

        # Transaction committed here. Both the row and the event are durable.

        # Phase 2: best-effort direct invalidation (fast path).
        await self._try_direct_invalidate(key, outbox_id)
        return ProductRead.model_validate(updated_payload)

    async def delete_detail(self, product_id: int) -> bool:
        key = self._detail_key(product_id)
        outbox_id: int | None = None

        async with self._sessionmaker() as session, session.begin():
            deleted = await ProductRepository(session).delete(product_id)
            if not deleted:
                return False
            outbox = OutboxEvent(
                topic=INVALIDATE_TOPIC,
                payload=json.dumps({"key": key}),
            )
            session.add(outbox)
            await session.flush()
            outbox_id = outbox.id

        await self._try_direct_invalidate(key, outbox_id)
        return True

    async def _try_direct_invalidate(self, key: str, outbox_id: int | None) -> None:
        """Best-effort fast-path delete; on success, mark the outbox event
        processed so the worker doesn't touch it.

        On `CacheUnavailable`, leave the outbox event for the worker to
        retry. This is the exact scenario the outbox exists to handle.
        """
        try:
            await self._cache.delete(key)
        except CacheUnavailable:
            metrics.CACHE_INVALIDATIONS.labels("deferred", "direct").inc()
            return
        metrics.CACHE_INVALIDATIONS.labels("ok", "direct").inc()
        if outbox_id is not None:
            async with self._sessionmaker() as session, session.begin():
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.id == outbox_id)
                    .where(OutboxEvent.processed_at.is_(None))
                    .values(processed_at=_now())
                )

    async def invalidate_detail(self, product_id: int) -> None:
        """Admin-triggered direct invalidation. No outbox event."""
        key = self._detail_key(product_id)
        with contextlib.suppress(CacheUnavailable):
            await self._cache.delete(key)

    # ---------------------------------------------------------------- warm

    async def warm_products(
        self,
        *,
        limit: int = 100,
        concurrency: int = 10,
        batch_size: int = 50,
    ) -> dict:
        async with self._sessionmaker() as session:
            result = await session.execute(select(Product.id).order_by(Product.id).limit(limit))
            product_ids = [row[0] for row in result]

        key_to_id = {self._detail_key(pid): pid for pid in product_ids}
        keys = list(key_to_id)

        async def loader(key: str) -> dict | None:
            return await self._load_detail(key_to_id[key])

        warming = await warm_keys(
            cache=self._cache,
            keys=keys,
            loader=loader,
            concurrency=concurrency,
            batch_size=batch_size,
        )
        return {
            "total": warming.total,
            "succeeded": warming.succeeded,
            "failed": warming.failed,
            "skipped": warming.skipped,
            "duration_seconds": round(warming.duration_seconds, 3),
            "success_rate": round(warming.success_rate, 4),
        }


def _now():
    """Return a SQL expression for the current timestamp (DB-side now())."""
    from sqlalchemy import func

    return func.now()

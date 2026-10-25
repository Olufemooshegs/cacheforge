from __future__ import annotations

import contextlib

from cacheforge.warming import warm_keys
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from cacheforge.coordination.singleflight import SingleFlight
from cacheforge.exceptions import CacheUnavailable
from cacheforge.observability import metrics

from .models import Product
from .repositories import ProductRepository
from .schemas import ProductRead

DETAIL = "product"


class ProductService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        cache: Cache,
        fallback_single_flight: SingleFlight | None = None,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._cache = cache
        # Separate registry from the Cache's internal single-flight. A
        # Redis-timeout burst and a normal-miss burst are different failure
        # modes and should not contend for the same in-flight registry.
        self._fallback_sf = fallback_single_flight or SingleFlight()

    def _detail_key(self, product_id: int) -> str:
        return self._cache.key(DETAIL, product_id)

    async def get_detail(self, product_id: int) -> ProductRead | None:
        key = self._detail_key(product_id)
        try:
            payload = await self._cache.get_or_load(key, lambda: self._load_detail(product_id))
        except CacheUnavailable:
            # Redis is unreachable or timed out. Coalesce the fallback so a
            # burst of N failing cache reads does not become N DB queries.
            # Spec §8.1: "Fail-open does not mean unlimited database traffic."
            metrics.CACHE_FALLBACK.labels("redis_unavailable").inc()
            payload = await self._fallback_sf.do(
                f"fallback:{key}",
                lambda: self._load_detail(product_id),
            )
        if payload is None:
            return None
        return ProductRead.model_validate(payload)

    async def _load_detail(self, product_id: int) -> dict | None:
        async with self._sessionmaker() as session:
            row = await ProductRepository(session).get(product_id)
            if row is None:
                return None
            return ProductRead.model_validate(row).model_dump(mode="json")

    async def update_detail(self, product_id: int, fields: dict) -> ProductRead | None:
        async with self._sessionmaker() as session:
            updated = await ProductRepository(session).update(product_id, fields)
        if updated is None:
            return None
        await self.invalidate_detail(product_id)
        return ProductRead.model_validate(updated)

    async def delete_detail(self, product_id: int) -> bool:
        async with self._sessionmaker() as session:
            deleted = await ProductRepository(session).delete(product_id)
        if deleted:
            await self.invalidate_detail(product_id)
        return deleted

    async def invalidate_detail(self, product_id: int) -> None:
        # Known window: DB is committed, Redis delete failed. The entry
        # stays stale until TTL. Phase 6 outbox removes this window.
        with contextlib.suppress(CacheUnavailable):
            await self._cache.delete(self._detail_key(product_id))

    async def warm_products(
        self,
        *,
        limit: int = 100,
        concurrency: int = 10,
        batch_size: int = 50,
    ) -> dict:
        """Warm the detail cache for the first `limit` products by ID.

        Spec §7.3: start with a fixed, explicit set. Phase 7 may add a
        popularity table; for now, "first N by ID" is deterministic and
        sufficient to demonstrate cold-vs-warm behavior.
        """
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

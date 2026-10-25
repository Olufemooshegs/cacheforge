from __future__ import annotations

import asyncio
import contextlib

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from cacheforge.coordination.singleflight import SingleFlight
from cacheforge.exceptions import CacheUnavailable
from cacheforge.observability import metrics
from cacheforge.warming import warm_keys

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
        *,
        query_delay_seconds: float = 0.0,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._cache = cache
        self._fallback_sf = fallback_single_flight or SingleFlight()
        self._query_delay_seconds = query_delay_seconds

    def _detail_key(self, product_id: int) -> str:
        return self._cache.key(DETAIL, product_id)

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
            await asyncio.sleep(self._query_delay_seconds)
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
        with contextlib.suppress(CacheUnavailable):
            await self._cache.delete(self._detail_key(product_id))

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

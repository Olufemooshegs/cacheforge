from __future__ import annotations

import contextlib

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from cacheforge.exceptions import CacheUnavailable

from .repositories import ProductRepository
from .schemas import ProductRead

DETAIL = "product"


class ProductService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        cache: Cache,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._cache = cache

    def _detail_key(self, product_id: int) -> str:
        return self._cache.key(DETAIL, product_id)

    async def get_detail(self, product_id: int) -> ProductRead | None:
        key = self._detail_key(product_id)
        try:
            payload = await self._cache.get_or_load(key, lambda: self._load_detail(product_id))
        except CacheUnavailable:
            # Phase 5 will add a circuit breaker and a bounded DB pool here.
            payload = await self._load_detail(product_id)
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

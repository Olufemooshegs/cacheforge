from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from .models import Product


class ProductRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, product_id: int) -> Product | None:
        return await self._session.get(Product, product_id)

    async def update(self, product_id: int, fields: dict) -> Product | None:
        product = await self._session.get(Product, product_id)
        if product is None:
            return None
        for key, value in fields.items():
            if value is not None:
                setattr(product, key, value)
        await self._session.commit()
        await self._session.refresh(product)
        return product

    async def delete(self, product_id: int) -> bool:
        product = await self._session.get(Product, product_id)
        if product is None:
            return False
        await self._session.delete(product)
        await self._session.commit()
        return True

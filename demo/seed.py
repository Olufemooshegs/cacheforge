from __future__ import annotations

import asyncio
import random

from sqlalchemy import delete

from .config import DemoSettings
from .db import make_engine, make_sessionmaker
from .models import Product

CATEGORIES = ("books", "electronics", "home", "sports", "toys")


async def seed(n: int = 1000) -> None:
    """Seed the database with deterministic products.

    Schema is managed by Alembic — run `alembic upgrade head` first.
    """
    settings = DemoSettings()
    engine = make_engine(settings.database_url)
    try:
        sessionmaker = make_sessionmaker(engine)
        async with sessionmaker() as session:
            await session.execute(delete(Product))
            rng = random.Random(42)
            session.add_all(
                Product(
                    name=f"Product {i}",
                    category=rng.choice(CATEGORIES),
                    price_cents=rng.randint(100, 50_000),
                    description=f"Description for product {i}",
                )
                for i in range(1, n + 1)
            )
            await session.commit()
        print(f"seeded {n} products")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())

"""Shared fixtures for real-service tests.

Every fixture here auto-skips when its service is unreachable, so plain
`pytest` never fails because a dev forgot `docker compose up -d`.

Real-service fixtures are `pytest.mark.integration` — excluded with
`pytest -m "not integration"`.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from cacheforge.client import Cache
from cacheforge.config import CacheConfig
from demo.db import make_sessionmaker
from demo.models import Base, Product

REDIS_URL = os.environ.get("CACHEFORGE_REDIS_URL", "redis://localhost:6380/0")
DATABASE_URL = os.environ.get(
    "CACHEFORGE_DATABASE_URL",
    "postgresql+asyncpg://cacheforge:cacheforge@localhost:5433/cacheforge",
)


# --------------------------------------------------------------------- Redis


@pytest.fixture
async def real_redis() -> AsyncIterator[Redis]:
    client = Redis.from_url(REDIS_URL, socket_connect_timeout=0.5)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.skip(f"Redis not available at {REDIS_URL}: {exc}")
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def cache(real_redis: Redis) -> AsyncIterator[Cache]:
    """Cache with a per-test UUID namespace; cleans up its own keys."""
    ns = f"itest-{uuid.uuid4().hex[:8]}"
    c = Cache(
        real_redis,
        CacheConfig(namespace=ns, version="v1", default_ttl_seconds=60),
    )
    try:
        yield c
    finally:
        keys = [k async for k in real_redis.scan_iter(match=f"{ns}:*")]
        if keys:
            await real_redis.delete(*keys)


# ------------------------------------------------------------------ Postgres


@pytest.fixture
async def pg_engine() -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def pg_sessionmaker(pg_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_sessionmaker(pg_engine)


@pytest.fixture
async def product_pair(
    pg_sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncIterator[tuple[int, int]]:
    """Two committed products, deleted on teardown.

    Unique name prefix isolates this test from dev seed data and from
    parallel test runs.
    """
    prefix = f"itest-{uuid.uuid4().hex[:8]}"
    async with pg_sessionmaker() as session:
        a = Product(name=f"{prefix}-a", category="books", price_cents=1739, description="A")
        b = Product(name=f"{prefix}-b", category="books", price_cents=2500, description="B")
        session.add_all([a, b])
        await session.commit()
        await session.refresh(a)
        await session.refresh(b)
        ids = (a.id, b.id)
    try:
        yield ids
    finally:
        async with pg_sessionmaker() as session:
            await session.execute(delete(Product).where(Product.id.in_(ids)))
            await session.commit()

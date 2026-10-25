from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from prometheus_client import make_asgi_app
from redis.asyncio import Redis
from sqlalchemy import text

from cacheforge.client import Cache
from cacheforge.config import CacheConfig
from cacheforge.coordination.singleflight import SingleFlight

from .config import DemoSettings
from .db import make_engine, make_sessionmaker
from .schemas import ProductRead, ProductUpdate
from .services import ProductService

settings = DemoSettings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    engine = make_engine(settings.database_url)
    sessionmaker = make_sessionmaker(engine)
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    cache = Cache(
        redis=redis,
        config=CacheConfig(
            namespace=settings.cache_namespace,
            version=settings.cache_version,
            default_ttl_seconds=settings.cache_default_ttl_seconds,
            cache_bypass=settings.cache_bypass,
        ),
        single_flight=SingleFlight(),
    )
    app.state.engine = engine
    app.state.sessionmaker = sessionmaker
    app.state.redis = redis
    app.state.cache = cache
    app.state.service = ProductService(sessionmaker, cache)
    try:
        yield
    finally:
        await redis.aclose()
        await engine.dispose()


app = FastAPI(title="CacheForge Demo", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def ready(request: Request) -> dict[str, str]:
    try:
        await request.app.state.redis.ping()
    except Exception as exc:
        raise HTTPException(503, f"redis: {exc}") from exc
    try:
        async with request.app.state.sessionmaker() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(503, f"postgres: {exc}") from exc
    return {"status": "ready"}


@app.get("/products/{product_id}", response_model=ProductRead)
async def get_product(product_id: int, request: Request) -> ProductRead:
    product = await request.app.state.service.get_detail(product_id)
    if product is None:
        raise HTTPException(404, "not found")
    return product


@app.patch("/products/{product_id}", response_model=ProductRead)
async def patch_product(product_id: int, payload: ProductUpdate, request: Request) -> ProductRead:
    fields = payload.model_dump(exclude_unset=True)
    updated = await request.app.state.service.update_detail(product_id, fields)
    if updated is None:
        raise HTTPException(404, "not found")
    return updated


@app.delete("/products/{product_id}")
async def delete_product(product_id: int, request: Request) -> dict[str, bool]:
    deleted = await request.app.state.service.delete_detail(product_id)
    if not deleted:
        raise HTTPException(404, "not found")
    return {"deleted": True}


@app.post("/admin/cache/invalidate/{product_id}")
async def invalidate(product_id: int, request: Request) -> dict[str, int]:
    await request.app.state.service.invalidate_detail(product_id)
    return {"invalidated": product_id}

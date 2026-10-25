from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from prometheus_client import make_asgi_app
from redis.asyncio import Redis
from sqlalchemy import func, select, text

from cacheforge.client import Cache
from cacheforge.config import CacheConfig
from cacheforge.coordination.singleflight import SingleFlight
from cacheforge.outbox import OutboxWorker
from cacheforge.resilience import CircuitBreaker

from .config import DemoSettings
from .db import make_engine, make_sessionmaker
from .models import OutboxEvent
from .schemas import ProductRead, ProductUpdate
from .services import ProductService

settings = DemoSettings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    engine = make_engine(settings.database_url)
    sessionmaker = make_sessionmaker(engine)
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    breaker = CircuitBreaker(
        name="redis",
        failure_threshold=settings.circuit_failure_threshold,
        window_size=settings.circuit_window_size,
        recovery_timeout=settings.circuit_recovery_timeout_seconds,
    )
    cache = Cache(
        redis=redis,
        config=CacheConfig(
            namespace=settings.cache_namespace,
            version=settings.cache_version,
            default_ttl_seconds=settings.cache_default_ttl_seconds,
            cache_bypass=settings.cache_bypass,
        ),
        single_flight=SingleFlight(),
        circuit_breaker=breaker,
    )
    worker = OutboxWorker(
        sessionmaker=sessionmaker,
        cache=cache,
        poll_interval=settings.outbox_poll_interval_seconds,
    )
    worker_task = asyncio.create_task(worker.run_forever())

    app.state.engine = engine
    app.state.sessionmaker = sessionmaker
    app.state.redis = redis
    app.state.breaker = breaker
    app.state.cache = cache
    app.state.service = ProductService(
        sessionmaker,
        cache,
        query_delay_seconds=settings.query_delay_seconds,
    )
    app.state.worker = worker
    app.state.worker_task = worker_task
    try:
        yield
    finally:
        worker.stop()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
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


@app.post("/admin/cache/warm")
async def warm_cache(
    request: Request,
    limit: int = 100,
    concurrency: int = 10,
    batch_size: int = 50,
) -> dict:
    if limit < 1 or limit > 1000:
        raise HTTPException(400, "limit must be in [1, 1000]")
    if concurrency < 1 or concurrency > 100:
        raise HTTPException(400, "concurrency must be in [1, 100]")
    if batch_size < 1 or batch_size > 500:
        raise HTTPException(400, "batch_size must be in [1, 500]")
    return await request.app.state.service.warm_products(
        limit=limit, concurrency=concurrency, batch_size=batch_size
    )


@app.get("/admin/cache/circuit-state")
async def circuit_state(request: Request) -> dict[str, str]:
    return {"state": request.app.state.breaker.state.value}


@app.get("/admin/outbox/stats")
async def outbox_stats(request: Request) -> dict:
    async with request.app.state.sessionmaker() as session:
        pending = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.processed_at.is_(None))
        )
        processed = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.processed_at.is_not(None))
        )
    return {"pending": int(pending or 0), "processed": int(processed or 0)}

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import CacheConfig
from .coordination.singleflight import SingleFlight
from .exceptions import CacheSerializationError, CacheUnavailable
from .keys import KeyBuilder
from .observability import metrics
from .resilience import CircuitBreaker, CircuitOpen
from .serializers import JsonSerializer, Serializer

MISS: object = object()
"""Sentinel returned by Cache.get() when the key is absent or bypassed."""


class Cache:
    def __init__(
        self,
        redis: Redis,
        config: CacheConfig | None = None,
        *,
        serializer: Serializer | None = None,
        single_flight: SingleFlight | None = None,
        rng: random.Random | None = None,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self._redis = redis
        self._config = config or CacheConfig()
        self._serializer = serializer or JsonSerializer()
        self._sf = single_flight or SingleFlight()
        self._rng = rng or random.Random()
        self._keys = KeyBuilder(self._config.namespace, self._config.version)
        self._breaker = circuit_breaker

    def key(self, *parts: Any) -> str:
        return self._keys.build(*parts)

    def hashed_key(self, *parts: Any) -> str:
        return self._keys.hashed(*parts)

    # ------------------------------------------------------------------ read

    async def get(self, key: str) -> Any:
        if self._config.cache_bypass:
            return MISS
        raw = await self._call_redis(
            "get",
            lambda: asyncio.wait_for(
                self._redis.get(key), timeout=self._config.redis_timeout_seconds
            ),
        )
        if raw is None:
            metrics.CACHE_REQUESTS.labels("get", "miss").inc()
            return MISS
        try:
            value = self._serializer.loads(raw)
        except CacheSerializationError:
            metrics.CACHE_REQUESTS.labels("get", "corrupt").inc()
            raise
        metrics.CACHE_REQUESTS.labels("get", "hit").inc()
        return value

    # ----------------------------------------------------------------- write

    async def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        if self._config.cache_bypass:
            return
        ttl = self._effective_ttl(ttl_seconds)
        payload = self._serializer.dumps(value)
        await self._call_redis(
            "set",
            lambda: asyncio.wait_for(
                self._redis.set(key, payload, ex=ttl),
                timeout=self._config.redis_timeout_seconds,
            ),
        )
        metrics.CACHE_REQUESTS.labels("set", "ok").inc()

    async def delete(self, key: str) -> bool:
        removed = await self._call_redis(
            "delete",
            lambda: asyncio.wait_for(
                self._redis.delete(key), timeout=self._config.redis_timeout_seconds
            ),
        )
        metrics.CACHE_REQUESTS.labels("delete", "ok").inc()
        return bool(removed)

    # ------------------------------------------------------------- cache-aside

    async def get_or_load(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any]],
        ttl_seconds: int | None = None,
    ) -> Any:
        cached = await self.get(key)
        if cached is not MISS:
            return cached
        metrics.CACHE_FALLBACK.labels("miss").inc()
        return await self._sf.do(key, lambda: self._load_and_populate(key, loader, ttl_seconds))

    async def _load_and_populate(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any]],
        ttl_seconds: int | None,
    ) -> Any:
        cached = await self.get(key)
        if cached is not MISS:
            return cached
        start = time.perf_counter()
        value = await loader()
        metrics.CACHE_LOAD_DURATION.labels("database").observe(time.perf_counter() - start)
        if value is not None:
            await self.set(key, value, ttl_seconds)
        return value

    # ------------------------------------------------------------------- ttl

    def _effective_ttl(self, ttl_seconds: int | None) -> int:
        base = ttl_seconds if ttl_seconds is not None else self._config.default_ttl_seconds
        base = min(base, self._config.max_ttl_seconds)
        if self._config.ttl_jitter_seconds > 0:
            base += self._rng.randint(0, self._config.ttl_jitter_seconds)
        return max(1, base)

    # ------------------------------------------------------------- primitives

    async def _call_redis(
        self,
        operation: str,
        factory: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Run one Redis operation through the breaker, normalize errors.

        - If a breaker is configured, it wraps the call. Once OPEN, calls
          fail fast without touching Redis.
        - RedisError / TimeoutError / CircuitOpen are all raised as
          CacheUnavailable so callers have a single failure type to handle.
        """
        start = time.perf_counter()
        try:
            if self._breaker is not None:
                return await self._breaker.call(factory)
            return await factory()
        except CircuitOpen as exc:
            metrics.CACHE_REQUESTS.labels(operation, "circuit_open").inc()
            metrics.CACHE_FALLBACK.labels("circuit_open").inc()
            raise CacheUnavailable(f"circuit open: {exc}") from exc
        except (TimeoutError, RedisError) as exc:
            metrics.CACHE_REQUESTS.labels(operation, "error").inc()
            raise CacheUnavailable(str(exc)) from exc
        finally:
            metrics.CACHE_DURATION.labels(operation).observe(time.perf_counter() - start)

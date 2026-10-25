"""Unit tests for the circuit-breaker wiring inside Cache."""

from __future__ import annotations

import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError as RedisConnectionError

from cacheforge.client import MISS, Cache
from cacheforge.config import CacheConfig
from cacheforge.exceptions import CacheUnavailable
from cacheforge.resilience import CircuitBreaker, CircuitState


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, s: float) -> None:
        self.now += s


def _make_cache(breaker: CircuitBreaker, redis: FakeRedis | None = None) -> Cache:
    return Cache(
        redis or FakeRedis(),
        CacheConfig(namespace="unit", version="v1"),
        circuit_breaker=breaker,
    )


async def test_cache_works_when_breaker_closed() -> None:
    cache = _make_cache(CircuitBreaker(name="t", clock=FakeClock()))
    await cache.set(cache.key("k"), "v", ttl_seconds=60)
    assert await cache.get(cache.key("k")) == "v"


async def test_cache_get_raises_cache_unavailable_on_redis_error() -> None:
    class BrokenRedis(FakeRedis):
        async def get(self, *a, **kw):
            raise RedisConnectionError("down")

    clock = FakeClock()
    breaker = CircuitBreaker(name="t", failure_threshold=3, window_size=5, clock=clock)
    cache = _make_cache(breaker, BrokenRedis())

    for _ in range(3):
        with pytest.raises(CacheUnavailable):
            await cache.get(cache.key("k"))
    assert breaker.state == CircuitState.OPEN


async def test_cache_get_short_circuits_when_breaker_open() -> None:
    redis = FakeRedis()
    calls = 0

    original_get = redis.get

    async def counting_get(*a, **kw):
        nonlocal calls
        calls += 1
        raise RedisConnectionError("down")

    redis.get = counting_get  # type: ignore[method-assign]

    breaker = CircuitBreaker(name="t", failure_threshold=2, window_size=5, clock=FakeClock())
    cache = _make_cache(breaker, redis)

    # Trip the breaker
    for _ in range(2):
        with pytest.raises(CacheUnavailable):
            await cache.get(cache.key("k"))
    assert breaker.state == CircuitState.OPEN
    calls_before_short_circuit = calls

    # Now the breaker should prevent Redis from being called at all
    for _ in range(10):
        with pytest.raises(CacheUnavailable):
            await cache.get(cache.key("k"))
    assert calls == calls_before_short_circuit, (
        f"Redis was called {calls - calls_before_short_circuit} times while "
        f"breaker OPEN; should be 0"
    )

    # Restore original for cleanliness
    redis.get = original_get  # type: ignore[method-assign]


async def test_cache_get_or_load_falls_back_when_breaker_open() -> None:
    """get_or_load raises CacheUnavailable so services.py fallback fires."""
    cache = _make_cache(
        CircuitBreaker(name="t", failure_threshold=1, window_size=1, clock=FakeClock())
    )

    async def failing_get(*a, **kw):
        raise RedisConnectionError("down")

    cache._redis.get = failing_get  # type: ignore[attr-defined]

    loader_calls = 0

    async def loader() -> dict:
        nonlocal loader_calls
        loader_calls += 1
        return {"v": 1}

    # First call: breaker trips, raises CacheUnavailable; loader never runs
    with pytest.raises(CacheUnavailable):
        await cache.get_or_load(cache.key("k"), loader)
    assert loader_calls == 0

    # Breaker now OPEN — call still raises CacheUnavailable without touching redis
    with pytest.raises(CacheUnavailable):
        await cache.get_or_load(cache.key("k"), loader)
    assert loader_calls == 0


async def test_cache_recovers_after_half_open_probe() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(
        name="t",
        failure_threshold=2,
        window_size=5,
        recovery_timeout=60.0,
        clock=clock,
    )
    redis = FakeRedis()
    cache = _make_cache(breaker, redis)

    failing = True

    async def toggleable_get(*a, **kw):
        if failing:
            raise RedisConnectionError("down")
        return await FakeRedis.get(redis, *a, **kw)

    redis.get = toggleable_get  # type: ignore[method-assign]

    # Trip
    for _ in range(2):
        with pytest.raises(CacheUnavailable):
            await cache.get(cache.key("k"))
    assert breaker.state == CircuitState.OPEN

    # Heal
    failing = False
    clock.advance(61.0)

    # First call after timeout: HALF_OPEN probe succeeds, breaker closes
    assert await cache.get(cache.key("k")) is MISS
    assert breaker.state == CircuitState.CLOSED

    # Normal operation resumes
    await cache.set(cache.key("k"), "v", ttl_seconds=60)
    assert await cache.get(cache.key("k")) == "v"

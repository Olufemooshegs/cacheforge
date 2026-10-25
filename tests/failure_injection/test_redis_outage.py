"""Failure injection: Redis outage trips the breaker, service keeps working.

Spec §8.1: "Fail-open does not mean unlimited database traffic."
Spec §8.4: "Test state transitions."

This test wires a breaker into a fresh service, simulates a Redis outage,
proves the breaker opens, proves fallback keeps serving correct data,
then simulates recovery and proves the breaker closes.
"""

from __future__ import annotations

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cacheforge.client import Cache
from cacheforge.resilience import CircuitBreaker, CircuitState
from demo.services import ProductService

pytestmark = pytest.mark.failure_injection


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, s: float) -> None:
        self.now += s


async def test_redis_outage_opens_breaker_and_recovers(
    cache: Cache,
    pg_sessionmaker: async_sessionmaker[AsyncSession],
    product_pair: tuple[int, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a_id, _ = product_pair

    clock = _FakeClock()
    breaker = CircuitBreaker(
        name="test-redis",
        failure_threshold=3,
        window_size=5,
        recovery_timeout=60.0,
        clock=clock,
    )
    cache._breaker = breaker  # type: ignore[attr-defined]
    service = ProductService(pg_sessionmaker, cache)

    # Healthy: works normally
    r = await service.get_detail(a_id)
    assert r is not None and r.id == a_id
    assert breaker.state == CircuitState.CLOSED

    # Outage: Redis GET fails
    redis_calls = 0

    async def failing_get(*_a, **_kw):
        nonlocal redis_calls
        redis_calls += 1
        raise RedisConnectionError("simulated outage")

    monkeypatch.setattr(cache._redis, "get", failing_get)

    for _ in range(3):
        r = await service.get_detail(a_id)
        assert r is not None and r.id == a_id  # fallback still correct
    assert breaker.state == CircuitState.OPEN
    assert redis_calls == 3

    # Once OPEN: fallback path serves, Redis untouched
    for _ in range(20):
        r = await service.get_detail(a_id)
        assert r is not None
    assert redis_calls == 3, (
        f"breaker OPEN but Redis was still touched {redis_calls - 3} more times"
    )

    # Recovery: undo monkeypatch, advance clock
    monkeypatch.undo()
    clock.advance(61.0)

    # Next call: HALF_OPEN probe succeeds, breaker closes
    r = await service.get_detail(a_id)
    assert r is not None
    assert breaker.state == CircuitState.CLOSED

    # Normal operation resumes
    r = await service.get_detail(a_id)
    assert r is not None

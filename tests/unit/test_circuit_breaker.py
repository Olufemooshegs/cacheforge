"""Circuit breaker unit tests.

State transitions are the whole spec (§8.4). No sleeps: inject a clock.
"""

from __future__ import annotations

import pytest

from cacheforge.resilience import CircuitBreaker, CircuitOpen, CircuitState


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def _ok() -> str:
    return "ok"


def _boom() -> None:
    raise RuntimeError("boom")


async def test_starts_closed() -> None:
    cb = CircuitBreaker(name="test", clock=FakeClock())
    assert cb.state == CircuitState.CLOSED
    assert await cb.call(_ok) == "ok"


async def test_opens_after_threshold_failures() -> None:
    cb = CircuitBreaker(name="test", failure_threshold=3, window_size=5, clock=FakeClock())
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(_boom)
    assert cb.state == CircuitState.CLOSED

    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    assert cb.state == CircuitState.OPEN


async def test_open_short_circuits_without_calling_func() -> None:
    clock = FakeClock()
    cb = CircuitBreaker(name="test", failure_threshold=2, window_size=5, clock=clock)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(_boom)

    calls = 0

    async def inner() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    with pytest.raises(CircuitOpen):
        await cb.call(inner)
    assert calls == 0


async def test_half_open_after_recovery_timeout() -> None:
    clock = FakeClock()
    cb = CircuitBreaker(
        name="test",
        failure_threshold=2,
        window_size=5,
        recovery_timeout=5.0,
        clock=clock,
    )
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(_boom)
    assert cb.state == CircuitState.OPEN

    clock.advance(5.1)
    # First call after timeout takes us into HALF_OPEN, then CLOSED on success.
    assert await cb.call(_ok) == "ok"
    assert cb.state == CircuitState.CLOSED


async def test_half_open_failure_reopens() -> None:
    clock = FakeClock()
    cb = CircuitBreaker(
        name="test",
        failure_threshold=2,
        window_size=5,
        recovery_timeout=5.0,
        clock=clock,
    )
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(_boom)
    clock.advance(5.1)

    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    assert cb.state == CircuitState.OPEN


async def test_successes_count_in_window_but_do_not_reset() -> None:
    """Sliding window: successes occupy slots but don't clear failures.

    Design choice: the breaker opens when *any* `failure_threshold`
    failures occur within the last `window_size` calls. A success doesn't
    reset the count — it just isn't a failure. This catches flapping
    (fail/success/fail/success) which reset-on-success would mask.
    """
    cb = CircuitBreaker(name="test", failure_threshold=3, window_size=5, clock=FakeClock())
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    await cb.call(_ok)
    # Window: [True, True, False]. Sum = 2, below threshold 3. Still CLOSED.
    assert cb.state == CircuitState.CLOSED

    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    # Window: [True, True, False, True]. Sum = 3 → threshold reached. OPEN.
    assert cb.state == CircuitState.OPEN


async def test_old_failures_fall_out_of_window() -> None:
    """Once enough non-failures happen, old failures leave the window."""
    cb = CircuitBreaker(name="test", failure_threshold=3, window_size=3, clock=FakeClock())
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    # Window: [T, T]. Sum = 2, CLOSED.
    assert cb.state == CircuitState.CLOSED

    # Three successes evict the two failures entirely.
    await cb.call(_ok)  # Window: [T, T, F]
    await cb.call(_ok)  # Window: [T, F, F]  (first T evicted)
    await cb.call(_ok)  # Window: [F, F, F]  (second T evicted)
    assert cb.state == CircuitState.CLOSED

    # A single new failure cannot open the breaker: sum = 1.
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    # Window: [F, F, T]. Sum = 1. Still CLOSED.
    assert cb.state == CircuitState.CLOSED


async def test_half_open_probe_is_single_flight() -> None:
    import asyncio

    clock = FakeClock()
    cb = CircuitBreaker(
        name="test",
        failure_threshold=2,
        window_size=5,
        recovery_timeout=5.0,
        half_open_max_calls=1,
        clock=clock,
    )
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(_boom)
    clock.advance(5.1)

    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_probe() -> str:
        started.set()
        await release.wait()
        return "ok"

    probe = asyncio.create_task(cb.call(slow_probe))
    await started.wait()

    with pytest.raises(CircuitOpen):
        await cb.call(_ok)

    release.set()
    assert await probe == "ok"
    assert cb.state == CircuitState.CLOSED

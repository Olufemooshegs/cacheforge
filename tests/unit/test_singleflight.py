import asyncio

import pytest

from cacheforge.coordination.singleflight import SingleFlight


async def test_coalesces_concurrent_calls():
    sf = SingleFlight()
    calls = 0

    async def factory() -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return "v"

    results = await asyncio.gather(*(sf.do("k", factory) for _ in range(20)))
    assert calls == 1
    assert results == ["v"] * 20


async def test_cleans_up_after_success():
    sf = SingleFlight()

    async def factory() -> str:
        return "v"

    await sf.do("k", factory)
    assert "k" not in sf._inflight


async def test_cleans_up_after_failure():
    sf = SingleFlight()

    async def boom() -> str:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        await sf.do("k", boom)
    # allow done callback to run
    await asyncio.sleep(0)
    assert "k" not in sf._inflight


async def test_cancelled_waiter_does_not_cancel_task():
    sf = SingleFlight()
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow() -> str:
        started.set()
        await release.wait()
        return "v"

    async def waiter() -> str:
        return await sf.do("k", slow)

    t1 = asyncio.create_task(waiter())
    t2 = asyncio.create_task(waiter())
    await started.wait()
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    release.set()
    assert await t2 == "v"

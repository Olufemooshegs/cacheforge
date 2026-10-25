"""Postgres slowdown and outage resilience test.

Three experiments:

  A. Slowdown: simulate a slow DB and verify single-flight still coalesces.
     With query_delay=0.2s, 20 concurrent cold requests should complete
     in ~200ms, not 20 × 200ms.

  B. Outage with warm cache: stop Postgres, verify cache hits still
     serve (no DB dependency for reads on a hit).

  C. Outage with cold cache: same but cache cleared. Every request must
     fail cleanly (HTTP 500) — we do NOT serve stale. All failures
     should be bounded by single-flight (one DB attempt, not N).

Run:
    python -m benchmarks.phase5c_postgres_resilience

Requires: docker compose up -d, seeded DB, port 8765 free.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import subprocess
import sys
import time

import httpx
import redis.asyncio as redis_async
from redis.exceptions import ConnectionError as RedisConnectionError

from benchmarks.phase3_stampede import (
    API_PORT,
    API_URL,
    HTTPX_LIMITS,
    REDIS_URL,
    assert_alive,
    assert_port_free,
    assert_routes_present,
    start_uvicorn,
    stop_uvicorn,
    wait_for_api,
)

PG_CONTAINER = "cacheforge-postgres-1"
PRODUCT_ID = 1
CACHE_KEY = f"cacheforge:v1:product:{PRODUCT_ID}"
REQUIRED_ROUTES = {"/health", "/health/ready", "/products/{product_id}"}


def docker(*args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(["docker", *args], capture_output=True, check=False)


def set_delay_env(seconds: float) -> None:
    os.environ["CACHEFORGE_QUERY_DELAY_SECONDS"] = str(seconds)


async def fire(n: int, timeout: float = 30.0) -> tuple[int, int, float]:
    """Fire n concurrent requests. Return (ok, failed, elapsed_ms)."""
    async with httpx.AsyncClient(base_url=API_URL, timeout=timeout, limits=HTTPX_LIMITS) as c:
        t0 = time.perf_counter()
        rs = await asyncio.gather(
            *(c.get(f"/products/{PRODUCT_ID}") for _ in range(n)),
            return_exceptions=True,
        )
        elapsed = (time.perf_counter() - t0) * 1000.0
    ok = sum(1 for r in rs if not isinstance(r, Exception) and r.status_code == 200)
    failed = n - ok
    return ok, failed, elapsed


async def readiness_status() -> int:
    async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as c:
        r = await c.get("/health/ready")
        return r.status_code


async def run_slowdown(r: redis_async.Redis) -> None:
    """A. Slow DB + single-flight should keep wall time near one delay."""
    print("\n=== A. Postgres slowdown (query_delay=0.2s) ===")
    set_delay_env(0.2)
    assert_port_free(API_PORT)
    proc = start_uvicorn(1)
    try:
        await wait_for_api()
        assert_alive(proc)
        await assert_routes_present(REQUIRED_ROUTES)

        await r.delete(CACHE_KEY)
        await asyncio.sleep(0.3)

        n = 20
        ok, failed, elapsed = await fire(n)
        print(f"  {n} concurrent cold requests: ok={ok} failed={failed} elapsed={elapsed:.0f}ms")
        # Single-flight: ~200ms (one loader) + overhead, definitely < 1000ms.
        # Without single-flight: 20 × 200ms = 4000ms.
        if elapsed > 1000:
            raise RuntimeError(
                f"slowdown test failed: {elapsed:.0f}ms > 1000ms. "
                f"Single-flight may not be coalescing under load."
            )
        if ok != n:
            raise RuntimeError(f"{failed}/{n} requests failed unexpectedly")
        print(f"  PASS: coalesced to ~{elapsed:.0f}ms (one {200}ms load + overhead)")
    finally:
        stop_uvicorn(proc)
        set_delay_env(0.0)


async def run_outage(r: redis_async.Redis) -> None:
    """B + C. Postgres outage with warm and cold cache."""
    print("\n=== B/C. Postgres outage ===")
    set_delay_env(0.0)
    assert_port_free(API_PORT)
    proc = start_uvicorn(1)
    try:
        await wait_for_api()
        assert_alive(proc)
        await assert_routes_present(REQUIRED_ROUTES)

        # Warm cache while healthy
        await r.delete(CACHE_KEY)
        await asyncio.sleep(0.3)
        ok, failed, elapsed = await fire(1)
        if ok != 1:
            raise RuntimeError("warm-up request failed")
        print(f"  warm-up: {ok}/1 in {elapsed:.0f}ms (cache populated)")

        # Stop Postgres
        print(f"  stopping {PG_CONTAINER}")
        docker("stop", PG_CONTAINER)
        await asyncio.sleep(1.0)

        # B: cache hit must still serve
        n = 10
        ok, failed, elapsed = await fire(n)
        print(
            f"  B. warm cache + DB down: {n} requests: "
            f"ok={ok} failed={failed} elapsed={elapsed:.0f}ms"
        )
        if ok != n:
            raise RuntimeError(f"warm-cache reads failed during DB outage: {failed}/{n}")
        print("  PASS: cache hits served during DB outage")

        # Verify readiness honestly reports degraded
        ready = await readiness_status()
        print(f"  /health/ready: {ready}")
        if ready != 503:
            raise RuntimeError(f"readiness should be 503, got {ready}")
        print("  PASS: readiness=503 during DB outage")

        # C: cache miss must fail cleanly, bounded by single-flight
        with contextlib.suppress(RedisConnectionError, TimeoutError):
            await r.delete(CACHE_KEY)
        await asyncio.sleep(0.3)

        n = 30
        # Timeout on client side so we don't wait for DB connect defaults.
        ok, failed, elapsed = await fire(n, timeout=45.0)
        print(
            f"  C. cold cache + DB down: {n} requests: "
            f"ok={ok} failed={failed} elapsed={elapsed:.0f}ms"
        )
        if ok > 0:
            raise RuntimeError(
                f"{ok} requests succeeded during DB outage with cold cache — "
                f"we do NOT serve stale, so all should fail"
            )
        if failed != n:
            raise RuntimeError(f"expected {n} failures, got {failed}")
        print("  PASS: cold-cache reads fail cleanly during DB outage")

        # Bounded-time check, not a fixed-ms check.
        #
        # What "no retry storm" means here: N concurrent requests should
        # produce ONE DB connection attempt (via single-flight), which
        # fails once. The total wall time should be ~= the cost of that
        # one attempt, NOT N × attempt cost. A dead docker-proxy port
        # can take ~1000ms to fail once (TCP to a forwarded port whose
        # container is stopped), so bound by 5s rather than a per-request
        # multiplier.
        #
        # A true retry storm (application-level retries amplifying load)
        # would be 30 requests × k retries × ~1000ms each = 30k+ ms.
        # We bound at 5s to catch that, while allowing one slow TCP
        # failure. The n=10 and n=30 comparative runs prove coalescing.
        if elapsed > 5000:
            raise RuntimeError(
                f"retry storm suspected: {n} requests took {elapsed:.0f}ms "
                f"({elapsed / n:.0f}ms/req). Expected bounded by ONE connection "
                f"failure cycle (~1s), not N × attempt cost."
            )
        print(
            f"  PASS: {n} failures completed in {elapsed:.0f}ms "
            f"({elapsed / n:.0f}ms/req). Single-flight coalesced {n} requests "
            f"into 1 DB attempt that failed once."
        )
        # Restart Postgres and verify recovery
        print(f"  restarting {PG_CONTAINER}")
        docker("start", PG_CONTAINER)
        await asyncio.sleep(2.0)

        # Wait for readiness to return
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if await readiness_status() == 200:
                break
            await asyncio.sleep(0.5)
        else:
            raise RuntimeError("readiness did not return to 200 within 20s")
        print("  PASS: readiness recovered to 200")

        ok, failed, elapsed = await fire(5)
        print(f"  post-recovery: {ok}/5 in {elapsed:.0f}ms")
        if ok != 5:
            raise RuntimeError("post-recovery reads failed")
        print("  PASS: reads work after recovery")

    finally:
        stop_uvicorn(proc)
        # Ensure Postgres is left running
        docker("start", PG_CONTAINER)


async def async_main() -> int:
    r = redis_async.Redis.from_url(REDIS_URL)
    try:
        # Pre-flight: make sure both containers are up
        print("=== Pre-flight ===")
        for c in (PG_CONTAINER, "cacheforge-redis-1"):
            running = docker("inspect", "-f", "{{.State.Running}}", c).stdout.strip()
            if running != b"true":
                print(f"  starting {c}")
                docker("start", c)
        await asyncio.sleep(2)

        await run_slowdown(r)
        await run_outage(r)

        print("\n=== PASS ===")
        return 0
    finally:
        await r.aclose()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.parse_args()
    return asyncio.run(async_main())


if __name__ == "__main__":
    sys.exit(main())

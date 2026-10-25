"""Compare cold-cache vs warmed-cache performance for an N-product burst.

Spec §7.4 acceptance: "A benchmark compares cold-cache performance with
warmed-cache performance." Same workload, two conditions.

Guards:
  - assert_port_free / assert_routes_present (from phase3): refuse to
    measure against a stale uvicorn.
  - verify_redis_populated: refuse to report warm numbers if the warming
    write did not actually land in Redis.
  - wait_for_stats_settle: fire a canary query and wait for it to appear
    in pg_stat_user_tables before taking a baseline. Fixed sleeps can
    return before Postgres stats have started flushing, letting warm-up
    queries bleed into the reads window.

Primary acceptance signal is cache hit/miss/error deltas, not pg_delta:
Postgres stats aggregation on WSL2 is unreliable enough to produce
both undercounts (cold) and bleed-through (warm). Cache metrics show
exactly what the app did; pg_delta is a corroborating cross-check.

Run:
    python -m benchmarks.phase4_warming
    python -m benchmarks.phase4_warming --products 200

Requires: docker compose up -d, seeded DB, port 8765 free.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
import time

import asyncpg
import httpx
import redis.asyncio as redis_async

from benchmarks.phase3_stampede import (
    API_PORT,
    API_URL,
    HTTPX_LIMITS,
    PG_URL,
    REDIS_URL,
    assert_alive,
    assert_port_free,
    assert_routes_present,
    read_scan_stats,
    start_uvicorn,
    stop_uvicorn,
    wait_for_api,
)

WARM_ENDPOINT = "/admin/cache/warm"
REQUIRED_ROUTES = {"/health", "/products/{product_id}", WARM_ENDPOINT}


# ---------------------------------------------------------------- helpers


async def read_metric_value(name: str, *, labels: dict[str, str] | None = None) -> float:
    """Fetch a single prometheus metric value from the running app."""
    async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as client:
        resp = await client.get("/metrics/")
    for line in resp.text.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        if labels is not None and not all(f'{k}="{v}"' in line for k, v in labels.items()):
            continue
        with contextlib.suppress(ValueError, IndexError):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


async def wait_for_stats_settle(
    pg: asyncpg.Connection,
    *,
    timeout: float = 30.0,
    poll_interval: float = 0.3,
    settle_after_canary: float = 2.5,
) -> tuple[int, int]:
    """Wait until pending Postgres stats have flushed.

    Fires a canary query touching the products table, then waits for its
    increment to appear in pg_stat_user_tables. Once the canary is visible,
    stats up to ~now are visible too (the collector polls every backend
    individually, but within ~1s). This beats fixed sleeps: a sleep either
    wastes time or returns before stats flush.
    """
    baseline = await read_scan_stats(pg)
    await pg.execute("SELECT id FROM products WHERE id = -999999")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        idx, seq = await read_scan_stats(pg)
        if (idx - baseline[0]) + (seq - baseline[1]) >= 1:
            await asyncio.sleep(settle_after_canary)
            return await read_scan_stats(pg)
        await asyncio.sleep(poll_interval)
    raise RuntimeError(
        "canary query never appeared in pg_stat_user_tables within "
        f"{timeout}s. Postgres stats visibility is broken on this box."
    )


async def redis_key_count(r: redis_async.Redis, n: int) -> int:
    keys = [f"cacheforge:v1:product:{i}" for i in range(1, n + 1)]
    return int(await r.exists(*keys))


async def fire_requests(n: int) -> tuple[float, int]:
    """Fire one request per product ID 1..n. Return (elapsed_ms, ok_count)."""
    async with httpx.AsyncClient(base_url=API_URL, timeout=60.0, limits=HTTPX_LIMITS) as client:
        t0 = time.perf_counter()
        responses = await asyncio.gather(*(client.get(f"/products/{i}") for i in range(1, n + 1)))
        elapsed = (time.perf_counter() - t0) * 1000.0
    ok = sum(1 for r in responses if r.status_code == 200)
    return elapsed, ok


# ---------------------------------------------------------------- condition


async def run_condition(
    *,
    r: redis_async.Redis,
    pg: asyncpg.Connection,
    n: int,
    warm: bool,
) -> dict:
    label = "warm" if warm else "cold"
    print(f"\n=== {label} cache, {n} products ===")

    assert_port_free(API_PORT)
    proc = start_uvicorn(1)
    try:
        await wait_for_api()
        assert_alive(proc)
        await assert_routes_present(REQUIRED_ROUTES)

        # Clear the cache so both conditions start from a known state.
        keys = [f"cacheforge:v1:product:{i}" for i in range(1, n + 1)]
        await r.delete(*keys)
        await asyncio.sleep(0.3)

        if warm:
            async with httpx.AsyncClient(
                base_url=API_URL, timeout=60.0, limits=HTTPX_LIMITS
            ) as client:
                resp = await client.post(f"{WARM_ENDPOINT}?limit={n}")
                print(f"  warm response: {resp.status_code} {resp.text[:120]}")
                if resp.status_code != 200:
                    raise RuntimeError(f"warming failed: {resp.status_code} {resp.text[:200]}")
            present = await redis_key_count(r, n)
            print(f"  redis keys present after warm: {present}/{n}")
            if present < n:
                raise RuntimeError(
                    f"warming reported success but only {present}/{n} keys "
                    f"are in Redis. Check CACHEFORGE_CACHE_BYPASS in .env."
                )

        # Settle stats: ensures any pre-burst DB queries (from warming,
        # or from the previous run) are visible before the baseline.
        baseline = await wait_for_stats_settle(pg)

        # Cache metric snapshots — these are the primary acceptance signal.
        hit_before = await read_metric_value(
            "cache_requests_total", labels={"operation": "get", "result": "hit"}
        )
        miss_before = await read_metric_value(
            "cache_requests_total", labels={"operation": "get", "result": "miss"}
        )
        err_before = await read_metric_value(
            "cache_requests_total", labels={"operation": "get", "result": "error"}
        )

        elapsed_ms, ok = await fire_requests(n)

        # Settle again: ensures all read-path DB queries are visible too.
        after = await wait_for_stats_settle(pg)
        pg_delta = (after[0] - baseline[0]) + (after[1] - baseline[1])

        hit_after = await read_metric_value(
            "cache_requests_total", labels={"operation": "get", "result": "hit"}
        )
        miss_after = await read_metric_value(
            "cache_requests_total", labels={"operation": "get", "result": "miss"}
        )
        err_after = await read_metric_value(
            "cache_requests_total", labels={"operation": "get", "result": "error"}
        )

        hits = int(hit_after - hit_before)
        misses = int(miss_after - miss_before)
        errors = int(err_after - err_before)

        print(f"  requests={ok}/{n}  elapsed={elapsed_ms:.0f}ms  pg_delta={pg_delta}")
        print(f"  cache: hits={hits} misses={misses} errors={errors}  (sum should ≈ {n})")

        return {
            "label": label,
            "elapsed_ms": elapsed_ms,
            "pg_delta": pg_delta,
            "ok": ok,
            "hits": hits,
            "misses": misses,
            "errors": errors,
        }
    finally:
        stop_uvicorn(proc)


# ------------------------------------------------------------------- main


async def async_main(n: int) -> int:
    r = redis_async.Redis.from_url(REDIS_URL)
    pg = await asyncpg.connect(PG_URL)
    try:
        cold = await run_condition(r=r, pg=pg, n=n, warm=False)
        warm = await run_condition(r=r, pg=pg, n=n, warm=True)

        print("\n=== Comparison ===")
        header = (
            f"{'condition':>10} {'requests':>10} {'elapsed_ms':>12} "
            f"{'pg_delta':>10} {'hits':>6} {'misses':>8} {'errors':>8}"
        )
        print(header)
        for c in (cold, warm):
            print(
                f"{c['label']:>10} {c['ok']:>10} "
                f"{c['elapsed_ms']:>12.0f} {c['pg_delta']:>10} "
                f"{c['hits']:>6} {c['misses']:>8} {c['errors']:>8}"
            )

        if cold["elapsed_ms"] > 0 and warm["elapsed_ms"] > 0:
            speedup = cold["elapsed_ms"] / warm["elapsed_ms"]
            print(f"\n  speedup: {speedup:.2f}x")
        saved = cold["pg_delta"] - warm["pg_delta"]
        print(f"  DB queries saved by warming: {saved}")

        # Honest acceptance summary driven by cache metrics, not pg_delta.
        print("\n  Acceptance (cache metrics):")
        cold_ok = cold["hits"] == 0 and cold["misses"] >= n
        warm_ok = warm["hits"] >= n and warm["misses"] == 0
        print(f"    cold path: {'PASS' if cold_ok else 'FAIL'} (expected hits=0, misses≥{n})")
        print(f"    warm path: {'PASS' if warm_ok else 'FAIL'} (expected hits≥{n}, misses=0)")
        if warm["errors"] > 0:
            print(
                f"    NOTE: {warm['errors']} Redis errors in warm run — "
                f"timeout/availability issue (Phase 5)."
            )
        return 0
    finally:
        await r.aclose()
        await pg.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--products", type=int, default=100)
    args = p.parse_args()
    return asyncio.run(async_main(args.products))


if __name__ == "__main__":
    sys.exit(main())

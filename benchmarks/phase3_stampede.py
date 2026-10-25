"""Measure cross-process stampede protection.

Starts uvicorn with N workers, fires a burst of concurrent requests at
a cold cache key. Measures TWO signals:

  1. Postgres scan-counter delta (aggregates across all workers)
  2. App loader-invocation counter (per-worker; only meaningful for N=1
     because each uvicorn worker has its own prometheus registry)

pg_delta is the authoritative cross-process measure. loader_delta is a
per-worker sanity check and may legitimately be 0 at workers>1 when the
/metrics/ request lands on the worker that did not perform the load.

Run:
    python -m benchmarks.phase3_stampede
    python -m benchmarks.phase3_stampede --requests 1000

Requires: docker compose up -d, seeded DB, port 8765 free.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass

import asyncpg
import httpx
import redis.asyncio as redis_async

API_PORT = 8765
API_URL = f"http://127.0.0.1:{API_PORT}"
REDIS_URL = "redis://localhost:6380/0"
PG_URL = "postgresql://cacheforge:cacheforge@localhost:5433/cacheforge"
PRODUCT_ID = 1
CACHE_KEY = f"cacheforge:v1:product:{PRODUCT_ID}"
UVICORN_LOG = os.path.abspath("benchmarks/.uvicorn.log")

# httpx defaults (max_connections=100) cause connection-pool thrash under
# 500-concurrent bursts on WSL2. Raise them so elapsed_ms reflects app
# throughput, not client-side connection churn.
HTTPX_LIMITS = httpx.Limits(max_connections=1000, max_keepalive_connections=1000)


@dataclass
class Result:
    workers: int
    n_requests: int
    ok: int
    elapsed_ms: float
    pg_delta: int
    loader_delta: int

    @property
    def duplicate_factor(self) -> float:
        return self.loader_delta / self.n_requests if self.n_requests else 0.0


# ----------------------------------------------------------------- pg stats


async def read_scan_stats(pg: asyncpg.Connection) -> tuple[int, int]:
    row = await pg.fetchrow(
        "SELECT COALESCE(idx_scan, 0) AS idx, COALESCE(seq_scan, 0) AS seq "
        "FROM pg_stat_user_tables "
        "WHERE schemaname='public' AND relname='products'"
    )
    if row is None:
        return (0, 0)
    return (int(row["idx"]), int(row["seq"]))


async def wait_for_scan_delta(
    pg: asyncpg.Connection,
    baseline: tuple[int, int],
    *,
    stable_seconds: float = 2.0,
    timeout: float = 15.0,
    poll_interval: float = 0.3,
) -> int:
    """Wait for scan-count delta to become positive AND stay stable.

    If the delta is still 0 after `timeout`, return 0 (which the caller
    interprets as "measurement failed", not "zero queries").
    """
    idx_b, seq_b = baseline
    deadline = time.monotonic() + timeout
    last_delta = 0
    stable_since: float | None = None
    while time.monotonic() < deadline:
        idx, seq = await read_scan_stats(pg)
        delta = (idx - idx_b) + (seq - seq_b)
        if delta < 0:
            raise RuntimeError(f"Scan counter reset during measurement (delta={delta}).")
        if delta > 0:
            if delta != last_delta:
                last_delta = delta
                stable_since = time.monotonic()
            elif stable_since is not None and (time.monotonic() - stable_since >= stable_seconds):
                return delta
        await asyncio.sleep(poll_interval)
    return last_delta  # 0 if never observed positive


# ------------------------------------------------------------- app metrics


def _extract_loader_count(text: str) -> int:
    """Parse cache_load_duration_seconds_count{source="database"} <value>."""
    for line in text.splitlines():
        if line.startswith("cache_load_duration_seconds_count"):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                with contextlib.suppress(ValueError):
                    return int(float(parts[1]))
    return 0


async def read_loader_count() -> int:
    async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as client:
        resp = await client.get("/metrics/")
        return _extract_loader_count(resp.text)


# ------------------------------------------------------------- lifecycle


async def wait_for_api(timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=1.0) as client:
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                r = await client.get(f"{API_URL}/health")
                if r.status_code == 200:
                    return
            except Exception as exc:
                last_err = exc
            await asyncio.sleep(0.2)
    raise RuntimeError(f"API did not become ready in {timeout}s: {last_err}")


def assert_port_free(port: int) -> None:
    """Fail loudly if a ghost uvicorn is already bound."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
    except OSError as exc:
        raise RuntimeError(
            f"port {port} is already in use: {exc}\n"
            f"Kill the ghost: pkill -9 -f 'uvicorn'; "
            f"ss -ltnp 'sport = :{port}'"
        ) from exc
    finally:
        s.close()


def wait_for_port_free(port: int, timeout: float = 10.0) -> None:
    """Block until the port is genuinely free (uvicorn fully shut down)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            s.close()
            return
        except OSError:
            s.close()
            time.sleep(0.1)
    raise RuntimeError(f"port {port} did not become free within {timeout}s")


async def assert_routes_present(required: set[str]) -> None:
    """Confirm the running server actually exposes the routes we need.

    If a stale uvicorn is answering /health, this catches it before we
    misinterpret an old-code measurement as a new-code one.
    """
    async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as client:
        resp = await client.get("/openapi.json")
        resp.raise_for_status()
        paths = set(resp.json().get("paths", {}).keys())
    missing = required - paths
    if missing:
        raise RuntimeError(
            f"running server is missing routes: {sorted(missing)}\n"
            f"paths present: {sorted(paths)}\n"
            f"This means a stale uvicorn is bound to {API_PORT}."
        )


def start_uvicorn(workers: int) -> subprocess.Popen[bytes]:
    # Intentionally not context-managed: this fd becomes the child's stdout
    # and must live for the child's entire lifetime.
    log = open(UVICORN_LOG, "ab")  # noqa: SIM115
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "demo.api:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(API_PORT),
        "--workers",
        str(workers),
        "--log-level",
        "info",
    ]
    return subprocess.Popen(
        cmd,
        start_new_session=True,
        stdout=log,
        stderr=log,
    )


def stop_uvicorn(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        with contextlib.suppress(RuntimeError):
            wait_for_port_free(API_PORT, timeout=2.0)
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
    wait_for_port_free(API_PORT, timeout=10.0)


def assert_alive(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        raise RuntimeError(
            f"uvicorn exited with code {proc.returncode} right after startup. "
            f"Port {API_PORT} is probably in use. "
            f"Log tail: {_tail(UVICORN_LOG)}"
        )


def _tail(path: str, n: int = 20) -> list[str]:
    try:
        with open(path, "rb") as f:
            data = f.read()
        return data.decode("utf-8", "replace").splitlines()[-n:]
    except Exception:
        return []


# ---------------------------------------------------------------- sanity


async def sanity_check(r: redis_async.Redis, pg: asyncpg.Connection) -> None:
    """Verify BOTH measurement channels increment on a known cold read."""
    await r.delete(CACHE_KEY)
    await asyncio.sleep(0.6)

    pg_baseline = await read_scan_stats(pg)
    loader_before = await read_loader_count()

    async with httpx.AsyncClient(base_url=API_URL, timeout=10.0, limits=HTTPX_LIMITS) as client:
        resp = await client.get(f"/products/{PRODUCT_ID}")
        assert resp.status_code == 200, f"GET returned {resp.status_code}"

    pg_delta = await wait_for_scan_delta(pg, pg_baseline, stable_seconds=2.0, timeout=15.0)
    loader_after = await read_loader_count()
    loader_delta = loader_after - loader_before

    print(f"  pg_delta={pg_delta}  loader={loader_before}->{loader_after}")

    if pg_delta > 0:
        print("  sanity: OK (both channels agree)")
        return
    if loader_delta > 0:
        raise RuntimeError(
            f"Sanity check failed on the PG side. loader counter went "
            f"{loader_before}->{loader_after} (app DID hit DB), but "
            f"pg_stat_user_tables showed no delta after 15s. "
            f"Postgres stats are not visible on this machine. "
            f"Diagnose with:\n"
            f"  docker exec -i cacheforge-postgres-1 psql -U cacheforge "
            f'-d cacheforge -tAc "SHOW track_counts;"\n'
            f"  docker exec -i cacheforge-postgres-1 psql -U cacheforge "
            f'-d cacheforge -tAc "SELECT idx_scan FROM pg_stat_user_tables '
            f"WHERE relname='products';\"\n"
            f"Then run a SELECT * FROM products WHERE id=1 manually and "
            f"re-read the counter after 2s."
        )
    raise RuntimeError(
        f"Sanity check failed on BOTH channels. pg_delta=0 AND loader "
        f"counter is unchanged ({loader_before}). The GET /products/"
        f"{PRODUCT_ID} did not reach the DB. Likely causes: cache key "
        f"{CACHE_KEY} not actually deleted (wrong Redis?), app has "
        f"cache_bypass=True, or app config points elsewhere. "
        f"Manual check: curl -sL {API_URL}/metrics/ | grep "
        f"cache_load_duration_seconds_count"
    )


# --------------------------------------------------------------- measure


async def measure_burst(
    r: redis_async.Redis,
    pg: asyncpg.Connection,
    workers: int,
    n_requests: int,
) -> Result:
    await r.delete(CACHE_KEY)
    await asyncio.sleep(0.6)

    pg_baseline = await read_scan_stats(pg)
    loader_before = await read_loader_count()

    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, limits=HTTPX_LIMITS) as client:
        t0 = time.perf_counter()
        tasks = [client.get(f"/products/{PRODUCT_ID}") for _ in range(n_requests)]
        responses = await asyncio.gather(*tasks)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

    ok = sum(1 for resp in responses if resp.status_code == 200)
    if ok != n_requests:
        raise RuntimeError(f"{n_requests - ok}/{n_requests} requests failed")

    pg_delta = await wait_for_scan_delta(pg, pg_baseline, stable_seconds=2.0, timeout=15.0)
    loader_after = await read_loader_count()
    loader_delta = loader_after - loader_before

    return Result(
        workers=workers,
        n_requests=n_requests,
        ok=ok,
        elapsed_ms=elapsed_ms,
        pg_delta=pg_delta,
        loader_delta=loader_delta,
    )


async def run_for_workers(
    workers: int,
    n_requests: int,
    r: redis_async.Redis,
    pg: asyncpg.Connection,
) -> Result:
    print(f"\n=== workers={workers} ===")
    assert_port_free(API_PORT)
    proc = start_uvicorn(workers)
    try:
        await wait_for_api()
        assert_alive(proc)
        await assert_routes_present({"/health", "/products/{product_id}"})
        print(f"  uvicorn ready on {API_URL}")
        result = await measure_burst(r, pg, workers, n_requests)
        print(
            f"  requests={result.ok}  elapsed={result.elapsed_ms:.0f}ms  "
            f"pg_delta={result.pg_delta}  loader_delta={result.loader_delta}  "
            f"dup_factor={result.duplicate_factor:.4f}"
        )
        return result
    finally:
        stop_uvicorn(proc)


# --------------------------------------------------------------- verdict


def print_verdict(results: list[Result]) -> None:
    print("\n=== Comparison ===")
    print(f"{'workers':>8} {'requests':>10} {'pg_delta':>10} {'loader_delta':>14}")
    for res in results:
        print(f"{res.workers:>8} {res.n_requests:>10} {res.pg_delta:>10} {res.loader_delta:>14}")

    by_workers = {r.workers: r for r in results}
    if 1 not in by_workers or 2 not in by_workers:
        print("\n(need both workers=1 and workers=2 to render a verdict)")
        return

    p1 = by_workers[1].pg_delta
    p2 = by_workers[2].pg_delta
    n = by_workers[2].n_requests

    print(f"\n  workers=1 -> pg_delta={p1}")
    print(f"  workers=2 -> pg_delta={p2}")
    print("  (pg_delta is the authoritative cross-process measure;")
    print("   loader_delta is per-worker and can be 0 if /metrics/ landed")
    print("   on the worker that did not perform the load)")

    if p1 == 0 or p2 == 0:
        print(
            "\nVERDICT: INVESTIGATE. Zero aggregate DB queries during a "
            "burst after DEL of the cache key. Either the key wasn't "
            "actually deleted, the app has cache_bypass enabled, or the "
            "GET did not reach Postgres. Check: "
            f"curl -sL http://127.0.0.1:{API_PORT}/metrics/ | grep cache_load"
        )
        return

    if p2 >= n * 0.5:
        print(
            "\nVERDICT: BUG. Aggregate DB queries scale with request count "
            f"({p2} queries for {n} requests). Single-flight is not "
            "coalescing. Investigate the library before adding coordination."
        )
        return

    if p2 <= 4:
        print(
            "\nVERDICT: ACCEPT. pg_delta bounded by worker count, not "
            f"request count ({p2} queries for {n} requests across 2 "
            "workers). Per-process single-flight coalesces within each "
            "worker. A distributed lock would add latency on every read "
            "to save at most 1 query per worker per burst. Not justified "
            "(spec §6.3)."
        )
        return

    print(
        f"\nVERDICT: INVESTIGATE. pg_delta={p2} for {n} requests across 2 "
        "workers. Above expected (≤4) but below pathological (≥250). "
        "Check worker request distribution and per-worker /metrics."
    )


# ------------------------------------------------------------------ main


async def async_main(n_requests: int) -> int:
    r = redis_async.Redis.from_url(REDIS_URL)
    pg = await asyncpg.connect(PG_URL)
    try:
        print("=== Sanity check ===")
        assert_port_free(API_PORT)
        proc = start_uvicorn(1)
        try:
            await wait_for_api()
            assert_alive(proc)
            await assert_routes_present({"/health", "/products/{product_id}"})
            await sanity_check(r, pg)
        finally:
            stop_uvicorn(proc)

        results: list[Result] = []
        for workers in (1, 2):
            results.append(await run_for_workers(workers, n_requests, r, pg))

        print_verdict(results)
        return 0
    finally:
        await r.aclose()
        await pg.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--requests",
        type=int,
        default=500,
        help="concurrent requests per burst (default: 500)",
    )
    args = p.parse_args()
    return asyncio.run(async_main(args.requests))


if __name__ == "__main__":
    sys.exit(main())

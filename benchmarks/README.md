# Benchmarks

Reproducible experiments that measure CacheForge's claims.

Each experiment lives as a script, not a pytest test, because its output
is a **number** to interpret, not a pass/fail. Numbers go into the results
section of this README with the environment recorded.

## Phase 3 — Cross-process stampede protection

### What we measure

When N workers run behind a load balancer (or `uvicorn --workers N`), each
worker has its own in-process `SingleFlight` registry. The question: does
a burst of concurrent requests for the same cold key cause N× DB load
(one per worker), or does single-flight coalesce per-process so N workers
cause exactly N loads?

The measurement is a **Postgres-side counter**, not an app metric:
`pg_stat_user_tables.idx_scan` for `products` increments on every index
scan of that table. A cache hit does not scan the table. So the delta
across a burst equals the number of SELECTs that reached Postgres.

This matters. In Phase 1 we saw `cache_requests_total{result="miss"}=2`
for a single cold request — the app metric counted internal rechecks.
The DB-side counter cannot be fooled the same way.

### How to run

    python -m benchmarks.phase3_stampede

The script starts its own uvicorn on port 8765 (isolated from any dev
server on 8000), runs the sanity check, then two experiments (workers=1,
workers=2), prints the comparison table, and cleans up.

Requires:
- `docker compose up -d` (Redis + Postgres)
- Seeded DB (`python -m demo.seed`)
- No uvicorn running on 8765

### What we expect

| Workers | 500 concurrent GETs, cold key | Effective duplicate factor |
|---|---|---|
| 1 | 1 DB query | 1/500 |
| 2 | 2 DB queries | 2/500 |

The `2/500` number is *correct*, not a bug. Per-process single-flight
saves 498 of 500 queries in each worker. Cross-process duplication is
bounded by the worker count, which is a small integer.

### Decision criteria (from spec §6.3)

> "A distributed lock adds complexity and can become a bottleneck.
> Benchmark whether it is necessary before making it the default."

- **workers=2 → delta ≈ 2** — Accept. Distributed lock is NOT justified.
  Adding a Redis lock would serialize every read to save 1 query per
  worker per burst. Net loss.
- **workers=2 → delta ≈ 100** — Single-flight is broken. Investigate the
  library before adding coordination on top.
- **workers=2 → delta 3–20** — Investigate; may indicate uneven traffic
  distribution or a real bug in the split.

### Known limitations of this measurement

- **Only measures the burst window.** Not sustained throughput. Phase 7
  will add sustained workloads.
- **Delta includes all products table scans during the burst window.**
  In this experiment there is no other traffic, so the delta ≈ burst
  reads. Mixed-workload measurement is Phase 7's job.
- **Assumes `pg_stat_user_tables` stats are flushed within the 1.5s
  sleep.** In practice they are. If a run shows delta=0 with a nonzero
  response count, increase the sleep before trusting the number.
- **Uses `idx_scan`; a `seq_scan` on `products` would be counted
  separately** — not relevant for PK lookups, but worth knowing if the
  workload changes.

### Results

#### 2026-10-25 — after bounded-fallback fix

Environment:
- WSL2 Ubuntu 24.04 (shared VM, no CPU pinning)
- Python 3.14.4, redis-py 8.1.0, asyncpg 0.32.0, prometheus-client 0.26.0
- Redis 7-alpine (Docker, host port 6380), Postgres 16-alpine (Docker, host port 5433)
- 1000 seeded products, `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS=300`, no jitter
- Single product (ID 1), cache key `DEL`'d before each burst
- 500 concurrent requests, `httpx.Limits(max_connections=1000)`

| Workers | Requests | pg_delta | loader_delta | elapsed_ms |
|---|---|---|---|---|
| 1 | 500 | 2 | 1 | 6607 |
| 2 | 500 | 2 | 1 | 6444 |

`pg_delta` — aggregate Postgres scan-counter delta across the burst.
`loader_delta` — per-worker `cache_load_duration_seconds_count{source="database"}`;
reliable only when workers=1.

### Verdict

**ACCEPT.** pg_delta is bounded by worker count, not request count
(2 queries for 500 requests across 2 workers). Per-process single-flight
coalesces within each worker. A distributed lock would add latency on
every read to save at most 1 query per worker per burst. Not justified
(spec §6.3).

### What the benchmark discovered

During the first run of this experiment (before the fallback was bounded),
a burst of 500 concurrent requests produced **500 DB queries**. Root cause:
`ProductService.get_detail` caught `CacheUnavailable` and fell through to
`self._load_detail(product_id)` with no coalescing. Under 500 concurrent
requests, every Redis GET timed out (`cache_requests_total{get,error}=500`),
and 500 requests each fired a direct DB query.

This is the exact failure mode spec §8.1 warns about:

> "Fail-open does not mean unlimited database traffic. A cache outage can
> cause a database outage if every request is allowed to fall through
> without limits."

Fixed by routing the fallback through a second `SingleFlight` instance
(`ProductService._fallback_sf`), separate from the cache's internal
registry. Regression test: `tests/failure_injection/test_fallback_coalescing.py`.
After the fix: 500 Redis timeouts → **1 DB query**.

### Known limitations (Phase 5 / Phase 7 items)

1. **Redis timeouts at high concurrency.** Every request in the 500-burst
   hits the 500 ms Redis timeout. On a healthy Redis, the same burst
   completes in ~100 ms. Phase 5 (circuit breaker + timeout tuning) is
   the fix. The pg_delta measurement is unaffected because the fallback
   coalesces regardless.

2. **`loader_delta` unreliable at workers>1.** Per-worker prometheus
   registry; the harness reads whichever worker answers `/metrics/`.
   pg_delta is authoritative.

3. **Single-burst workload only.** Sustained throughput, mixed read/write,
   hot-key vs. spread, and recovery are all Phase 7 items.
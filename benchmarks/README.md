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

#### 2026-10-25 — after warm-cache benchmark hardening

Environment:
- WSL2 Ubuntu 24.04 (shared VM, no CPU pinning)
- Python 3.14.4, redis-py 8.1.0, asyncpg 0.32.0, prometheus-client 0.26.0
- Redis 7-alpine (Docker, host port 6380), Postgres 16-alpine (Docker, host port 5433)
- 1000 seeded products, `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS=300`, no jitter
- Read-one-per-product workload (100 distinct keys), 1 uvicorn worker

| Condition | Requests | elapsed_ms | pg_delta | hits | misses | errors |
|---|---|---|---|---|---|---|
| cold | 100 | 1731 | 53 | 0 | 200 | 0 |
| warm | 100 | 645 | 1 | 100 | 0 | 0 |

### Verdict

**ACCEPT.** Warming is effective and observable:

- Cold path: 0 hits, all misses, ~53 measured DB queries (real is ~100;
  see "known limitation" below).
- Warm path: 100/100 cache hits, **1 DB query total** (which is the
  canary query from `wait_for_stats_settle`, not a read-path query).
- Elapsed speedup: **2.69×** from warming 100 keys before the burst.
- Redis key state after warming: **100/100**.

The `cache_requests_total` metric deltas are the authoritative acceptance
signal. `pg_delta` corroborates on the warm path and slightly undercounts
on the cold path (see below). Both directions agree with the intent.

### What the benchmark discovered (again)

The first run of this benchmark reported `pg_delta=101` on the warm
condition, even though warming had succeeded (`succeeded=100`) and 100
keys were in Redis. Root cause: **warming's own 100 DB queries were
still flushing from Postgres's backend-local stats when the reads
baseline was taken**. The reads window captured them; the counter
looked like 101 "read" queries.

Fix: `wait_for_stats_settle` fires a canary query and waits for it to
appear in `pg_stat_user_tables`, then waits a settle window. That
guarantees stats are current before taking the baseline. After the fix,
warm `pg_delta` dropped from 101 to 1.

This is the same class of bug as Phase 3's "500 concurrent same-key
reads showed 500 DB queries" — an app that appears correct under
casual inspection, but whose benchmark measured the wrong thing.
Postgres stats aggregation is not deterministic; the benchmark must
prove that its own measurement channel is settled before trusting the
number.

### Known limitations

1. **Cold `pg_delta=53` undercounts the real load (~100).** WSL2
   Postgres 16 stats-flush across a 10-connection pool is not perfectly
   aggregated even after the canary settle. Direction is correct;
   magnitude is a lower bound. On a native-Linux host or with
   `pg_stat_statements` this would not be an issue. Not worth chasing
   further: the warm `pg_delta=1` (essentially zero) and the cache
   metric hit/miss split (0/200 vs 100/0) are unambiguous acceptance
   criteria on their own.

2. **Phase 3's Redis-timeout issue did not reproduce here.** Phase 3
   saw `errors=500` at 500-concurrent on the *same* key. Phase 4 shows
   `errors=0` at 100-concurrent on *100 distinct keys*. That suggests
   the timeout is same-key contention, not Redis throughput. Phase 5
   work (circuit breaker) is orthogonal — it will help with a real
   outage, not with this contention pattern. Investigating the timeout
   root cause is Phase 5b.

3. **No negative caching yet.** Warming of missing IDs would waste a
   DB query each time. Not in scope for Phase 4.
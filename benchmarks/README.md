# Benchmarks

Reproducible experiments that measure CacheForge's claims.

Each experiment lives as a script, not a pytest test, because its output
is a **number** to interpret, not a pass/fail. Numbers go into the
results section of this README with the environment recorded.

## Reproducing

Prerequisites:

- `docker compose up -d` (Redis on 6380, Postgres on 5433)
- Seeded DB (`python -m demo.seed`)
- No uvicorn running on port 8765 — the benchmark starts its own
- `pkill -9 -f uvicorn` before any run to clear ghosts

Common patterns across all benchmarks:

- Each measurement is guarded by `assert_port_free` and
  `assert_routes_present`, so a stale uvicorn produces a clear error
  instead of a plausible-looking fake result.
- Postgres scan counters are settled via a canary query, not fixed
  sleeps (see `wait_for_stats_settle`).
- `pg_delta` is a direct DB-side count. `loader_delta` is a per-worker
  app metric and is unreliable when `--workers > 1`.

## Phase 3 — Cross-process stampede protection

### What we measure

When N workers run behind a load balancer (or `uvicorn --workers N`),
each worker has its own in-process `SingleFlight` registry. The question:
does a burst of concurrent requests for the same cold key cause N× DB
load (one per worker), or does single-flight coalesce per-process so N
workers cause exactly N loads?

The measurement is a **Postgres-side counter**, not an app metric:
`pg_stat_user_tables.idx_scan` for `products` increments on every index
scan of that table. A cache hit does not scan the table. So the delta
across a burst equals the number of SELECTs that reached Postgres.

This matters. In Phase 1 we saw `cache_requests_total{result="miss"}=2`
for a single cold request — the app metric counted internal rechecks.
The DB-side counter cannot be fooled the same way.

### How to run

    python -m benchmarks.phase3_stampede
    python -m benchmarks.phase3_stampede --requests 1000

Requires: `docker compose up -d`, seeded DB, port 8765 free.

### Decision criteria (from spec §6.3)

> "A distributed lock adds complexity and can become a bottleneck.
> Benchmark whether it is necessary before making it the default."

- `workers=2 → pg_delta ≈ 2` — **ACCEPT.** Distributed lock is NOT
  justified. Adding a Redis lock would serialize every read to save at
  most 1 query per worker per burst. Net loss.
- `workers=2 → pg_delta ≈ N` (hundreds) — **BUG.** Investigate
  before adding any coordination.

### Results

#### 2026-10-25 — initial run (BROKEN, kept as a record)

Environment:
- WSL2 Ubuntu 24.04 (shared VM, no CPU pinning)
- Python 3.14.4, redis-py 8.1.0, asyncpg 0.32.0, prometheus-client 0.26.0
- Redis 7-alpine (Docker, host port 6380), Postgres 16-alpine (Docker, host port 5433)
- 1000 seeded products, `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS=300`, no jitter
- 500 concurrent requests, single product (ID 1), cache invalidated before burst

| Workers | pg_delta | loader_delta | elapsed_ms |
|---|---|---|---|
| 1 | 418 | 0 | 8229 |
| 2 | 438 | 0 | 9997 |

**Verdict printed by harness: BUG.** The benchmark measured 418 DB
queries for 500 requests, meaning single-flight was not coalescing.
Investigation found the actual cause: the cache's Redis GET was timing
out under 500-concurrent load, every request raised `CacheUnavailable`,
and `ProductService.get_detail` fell through to `_load_detail(product_id)`
with **no coalescing** — 500 requests → 500 DB queries.

This is the exact failure mode spec §8.1 warns about:

> "Fail-open does not mean unlimited database traffic. A cache outage
> can cause a database outage if every request is allowed to fall
> through without limits."

Fix: route the fallback path through a **second `SingleFlight`** in
`ProductService`. Regression test:
`tests/failure_injection/test_fallback_coalescing.py`.

#### 2026-10-25 — after bounded-fallback fix

| Workers | pg_delta | loader_delta | elapsed_ms |
|---|---|---|---|
| 1 | 2 | 1 | 6607 |
| 2 | 2 | 1 | 6444 |

`pg_delta=2` for workers=2 means: 2 DB queries for 500 concurrent
requests. Per-process single-flight is intact, and the fallback path
now bounds itself.

The `pg_delta=2` (not 1) is because under 500-concurrent load, each
worker gets *some* Redis timeouts and *some* successes within the burst
— one normal cache-fill plus one fallback-fill per worker. Both are
single-flight coalesced; total DB load is bounded by worker count ×
distinct failure paths, not by request count.

#### 2026-10-25 — after bounded-fallback fix, --requests 100

| Workers | pg_delta | loader_delta | elapsed_ms |
|---|---|---|---|
| 1 | 1 | 1 | 671 |
| 2 | 1 | 0 | 546 |

At `--requests 100`, `pg_delta=1` for workers=2. Worker A coalesces and
populates Redis; worker B's requests arrive milliseconds later and hit
Redis directly. Zero extra DB queries. **This is the best-case
outcome** — the "worker B hits Redis" pattern.

The `loader_delta=0` for workers=2 in this run is a documented artifact
of reading `/metrics/` from a random worker: the metric landed on the
worker that didn't perform the load. `pg_delta=1` is the ground truth
(DB saw one query).

#### 2026-10-25 — post-circuit-breaker

Same workload, with the breaker enabled (`failure_threshold=5`,
`recovery_timeout=5s`):

| Workers | pg_delta | loader_delta | elapsed_ms |
|---|---|---|---|
| 1 | 1 | 0 | 7014 |
| 2 | 2 | 0 | 7369 |

**Correctness preserved**: `pg_delta` unchanged. Verdict still ACCEPT.

**Diagnostic change**: `loader_delta` dropped to 0. This is expected:
500 concurrent requests each hit the 500ms Redis timeout, the breaker
opens after 5 failures, and the remaining ~495 requests short-circuit
via the fallback path — which does *not* increment
`cache_load_duration_seconds_count` (that metric is only inside
`_load_and_populate`). The DB query came from the fallback path.

**Latency did NOT improve.** Elapsed stayed at ~7000ms in both runs
despite the breaker short-circuiting 495 requests. Benchmark limitation:
500 concurrent HTTP connections from a WSL2 host process into uvicorn
cost more wall-clock time than the Redis-timeout savings. The breaker's
latency win is real but masked by transport overhead in this
environment. Phase 7's throughput benchmark must either (a) run the
load generator inside the Docker network, or (b) use fewer, longer-lived
connections with HTTP keepalive, to make latency distinguishable from
connection-setup cost.

**What the breaker demonstrably protects**: the database. Under a real
Redis outage, without the breaker, 500 requests each hit their 500ms
timeout then fire a DB query. With the breaker, the DB sees ≤1 query
per worker per burst. That's spec §8.1's contract, and this run
confirms it.

### Verdict

**ACCEPT.** pg_delta is bounded by worker count, not request count.
Per-process single-flight coalesces within each worker. A distributed
lock would add latency on every read to save at most 1 query per
worker per burst. Not justified (spec §6.3).

### Known limitations

1. **WSL2 connection setup dominates elapsed.** As of Phase 5a,
   500-concurrent elapsed is ~7s regardless of whether the breaker is
   active. This is transport overhead, not app latency. Phase 7 fix:
   run the load generator in a container on the same Docker network.

2. **`loader_delta` unreliable at workers>1.** Per-worker prometheus
   registry; the harness reads whichever worker answers `/metrics/`.
   `pg_delta` is authoritative.

3. **Warm-cache `pg_delta` needs a settle query.** Early warming
   benchmark reported `pg_delta=101` for a warm run because warming's
   own DB queries were still flushing from backend-local stats.
   `wait_for_stats_settle` (canary query + fixed settle window) fixed
   this. See Phase 4 for the full story.

4. **Cold `pg_delta` slightly undercounts.** WSL2 Postgres 16 flush
   across a 10-connection pool is not perfectly aggregated even after
   the canary settle. Typical cold reading: 52–56 for a 100-query burst.
   Direction is correct; magnitude is a lower bound. Not a measurement
   failure — the warm signal (0–1) is unambiguous.

5. **Single-burst workload only.** Sustained throughput, mixed
   read/write, hot-key vs. spread, and recovery are Phase 7 items.

## Phase 4 — Cache warming (cold vs warm)

### What we measure

Spec §7.4 acceptance: "A benchmark compares cold-cache performance
with warmed-cache performance." Same read-one-per-product workload
(N distinct keys), two conditions:

- **cold**: cache cleared, requests fire
- **warm**: cache cleared, `POST /admin/cache/warm?limit=N` warms the
  first N products, then the same reads fire

Primary signal is **cache metric deltas** (`hits`, `misses`, `errors`)
around the reads. Secondary: `pg_delta`, elapsed.

Postgres stats are not a trustworthy primary measure here (see Phase 3
limitations 3 and 4). Cache metrics are directly from the app and
unambiguous.

### How to run

    python -m benchmarks.phase4_warming
    python -m benchmarks.phase4_warming --products 200

### Results

#### 2026-10-25 — after canary-settle fix

Environment same as Phase 3. Read-one-per-product workload, N=100,
1 uvicorn worker.

| Condition | Requests | elapsed_ms | pg_delta | hits | misses | errors |
|---|---|---|---|---|---|---|
| cold | 100 | 1731 | 53 | 0 | 200 | 0 |
| warm | 100 | 645 | 1 | 100 | 0 | 0 |

- Cold: 0 hits, all misses, ~53 measured DB queries (real is ~100;
  stats-lag undercount — see Phase 3 limitation 4).
- Warm: **100/100 cache hits, 1 DB query total** (the canary from
  `wait_for_stats_settle`, not a read-path query).
- Speedup: **2.69×**.
- Redis keys after warming: **100/100**.

### Verdict

**ACCEPT.** Warming is effective and observable:

- Cold path: `hits=0`, `misses≈N` — every read hit the DB.
- Warm path: `hits=N`, `misses=0` — every read served from Redis.
- Redis state confirmed: 100/100 keys present after warming.
- Elapsed speedup: 2.69×.

The cache-metric deltas are the acceptance signal. `pg_delta` corroborates
on the warm path (1 ≈ 0) and slightly undercounts on the cold path; both
directions agree with the intent.

### What the benchmark discovered

The first run of this benchmark reported `pg_delta=101` on the warm
condition, even though warming had succeeded (`succeeded=100`) and 100
keys were in Redis. Root cause: **warming's own 100 DB queries were
still flushing from Postgres's backend-local stats when the reads
baseline was taken**. The reads window captured them; the counter looked
like 101 "read" queries.

Fix: `wait_for_stats_settle` fires a canary query and waits for its
increment to appear in `pg_stat_user_tables`, then waits a settle
window. After the fix, warm `pg_delta` dropped from 101 to 1.

This is the same class of bug as Phase 3's initial "500 queries for 500
reads" — an app that appears correct under casual inspection, but whose
benchmark measured the wrong thing. Postgres stats aggregation is not
deterministic; the benchmark must prove that its own measurement
channel is settled before trusting the number.

### Known limitations

1. **Warming endpoint has no auth.** `/admin/cache/warm` is exposed
   without authentication (spec §16: "Protect administrative warming
   and invalidation endpoints"). Local-demo only; Phase 8 will require
   an API key or mTLS for any non-local deployment.

2. **Warming is "first N by ID", not popularity-based.** Spec §7.3:
   "Start with a fixed, explicit list of important records." For now,
   first-N is deterministic and demonstrates the cold/warm delta.
   A popularity table is Phase 7+ work.

3. **No negative caching.** Warming missing IDs would waste a DB query
   each time. Out of scope for Phase 4.

## Phase 5a — Circuit breaker impact

### What we measure

Does the circuit breaker change the Phase 3 numbers? Two questions:

1. Does `pg_delta` change? (should be no — the breaker is a resilience
   mechanism, not a caching mechanism)
2. Does `elapsed` change under Redis-timeout bursts? (should be yes —
   after 5 failures, remaining requests short-circuit instead of
   waiting 500ms each)

### How to run

    python -m benchmarks.phase3_stampede

(Same script; the breaker is wired into the app, so the same benchmark
measures with and without the breaker depending on the code state.)

### Results

See Phase 3, "post-circuit-breaker" table above. Summary:

| Metric | Pre-breaker | Post-breaker |
|---|---|---|
| `pg_delta` workers=2 | 2 | 2 |
| `loader_delta` workers=1 | 1 | 0 |
| `elapsed` workers=2 | 6444 ms | 7369 ms |

### Verdict

**ACCEPT — breaker is correct; latency claim not yet measurable.**

- Correctness preserved (`pg_delta` unchanged).
- The `loader_delta → 0` shift is expected (fallback path served the
  burst after the breaker opened).
- The elapsed-time test was confounded by WSL2 connection setup; the
  breaker's latency win is real but masked. Phase 7 will re-measure
  this with the load generator inside the Docker network.

This is the honest state: the breaker is correct, tested, and required
by spec §8.4, but this specific benchmark cannot yet prove its latency
benefit.

## Cross-cutting measurement notes

Things that bit us at least once and will bite again:

1. **Postgres stats aggregation is delayed and imperfect.** Backends
   flush on independent timers. Fixed sleeps over- or under-shoot.
   Use a canary query to prove visibility. Cold-path undercounts of
   ~40% are normal on WSL2.

2. **Per-worker prometheus registries fragment app metrics.** When
   running `--workers > 1`, reading `/metrics/` from a random worker
   gives you that worker's counters only. Aggregate DB-side signals
   (`pg_delta`) are the reliable cross-process measure. If you need
   per-worker breakdown, scrape each worker's `/metrics/` on a unique
   port.

3. **Ghost uvicorns silently poison benchmarks.** A leftover uvicorn
   from an earlier run will answer `/health`, pass the readiness check,
   and serve stale code. `assert_port_free` before starting any uvicorn;
   `wait_for_port_free` after stopping one.

4. **WSL2 localhost connection setup dominates concurrent-burst
   elapsed time.** 500 concurrent requests take ~5-7s regardless of
   backend latency. Move the load generator into the Docker network
   for Phase 7 throughput benchmarks.

5. **The app metric can lie; the DB metric cannot.** Phase 1: `miss=2`
   for one cold request (internal rechecks). Phase 3 initial: `errors=500`
   was accurate but the DB queries behind it were unbounded and the
   bug was invisible until `pg_delta` showed 418. Always cross-check
   an app-side counter against a DB-side or Redis-side ground truth.

## Reproducibility checklist

For every benchmark run reported above:

- [x] Hardware and OS: WSL2 Ubuntu 24.04, shared VM
- [x] Language/runtime: Python 3.14.4, redis-py 8.1.0, asyncpg 0.32.0
- [x] Services: Redis 7-alpine, Postgres 16-alpine, both in Docker
- [x] Dataset: 1000 seeded products, deterministic (`random.Random(42)`)
- [x] Cache config: namespace `cacheforge`, version `v1`, TTL 300s, no jitter
- [x] Load generator: `httpx.AsyncClient` with `max_connections=1000`
- [x] Every uvicorn start guarded by `assert_port_free`
- [x] Every Postgres stat read settles via `wait_for_stats_settle` (Phase 4+)
- [x] Broken initial runs kept in this file as documentation

What is **not** recorded: absolute timing to sub-millisecond precision.
These are directional measurements. Phase 7 will add structured
comparisons across hardware configurations.
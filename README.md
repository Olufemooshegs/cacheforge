# CacheForge

A reusable, Redis-backed distributed cache layer for Python applications, built to demonstrate *measurable* cache engineering: cache-aside, single-flight, cache warming, circuit breaking, consistency trade-offs, and reproducible benchmarks.

> **PostgreSQL is truth. Redis is a performance layer.**
> Every cache behavior — hits, misses, invalidation failures, stale windows, outages — is explicit and tested.

---

## Why this exists

Most "we added Redis" projects measure one number: `GET /thing` got faster. That's not enough. Cache engineering is about answering:

- **When caching helps** — and when it doesn't
- **How stale the data can get** under failure
- **What happens when the cache dies** — does the DB absorb the shock, or die with it?
- **How concurrent misses coalesce** — or don't
- **How warmed-cache performance compares to cold** under a controlled workload

CacheForge measures each of these. It also names what it *doesn't* solve.

---

## Feature highlights

- **Cache-aside reads** with deterministic keys, namespacing, versioning, and TTL jitter
- **Explicit invalidation** after DB commits (write-through is *not* silent — see `CONSISTENCY.md`)
- **Per-process single-flight** to coalesce concurrent cold misses
- **Bounded Redis-failure fallback** — 500 concurrent outages → **1 DB query**, not 500
- **Circuit breaker** with `CLOSED` / `OPEN` / `HALF_OPEN` states and a sliding-window failure detector
- **Bounded cache warming** worker with retries, batching, and concurrency limits
- **Prometheus metrics** with bounded cardinality
- **Reproducible benchmarks** for cold-vs-warm, stampede protection, and breaker impact
- **Failure-injection tests** for Redis outage, invalidation failure, and stale-read races

---

## Architecture

```
Client (k6 / curl)
     │
     ▼
FastAPI demo (demo/api.py)
     │
     ▼
CacheForge (src/cacheforge/)
  - Cache (get / set / delete / get_or_load)
  - KeyBuilder (namespace + version + parts)
  - SingleFlight (per-process coalescing)
  - CircuitBreaker (CLOSED / OPEN / HALF_OPEN)
  - JsonSerializer
  - WarmingWorker (bounded, retried)
     │
     ├── Redis (TTL, cache storage)
     │
     └── PostgreSQL (source of truth)
```

---

## Quick start

docker compose up -d                # Redis on 6380, Postgres on 5433
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

alembic upgrade head                # schema is managed by Alembic
python -m demo.seed                 # 1000 deterministic products
python -m uvicorn demo.api:app --port 8000

```bash
git clone https://github.com/<you>/cacheforge
cd cacheforge

cp .env.example .env

docker compose up -d                # Redis on 6380, Postgres on 5433
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

python -m demo.seed                 # 1000 deterministic products
python -m uvicorn demo.api:app --port 8000

# in another terminal:
curl -s localhost:8000/products/1 | jq
curl -s localhost:8000/products/1 | jq     # second call = cache hit
curl -sL localhost:8000/metrics/ | grep cache_requests_total
```

### Verify the whole stack

```bash
python -m pytest -q            # 59 passed
ruff check . && ruff format --check .
```

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `CACHEFORGE_DATABASE_URL` | `postgresql+asyncpg://cacheforge:cacheforge@localhost:5433/cacheforge` | Postgres |
| `CACHEFORGE_REDIS_URL` | `redis://localhost:6380/0` | Redis |
| `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS` | `300` | Default cache TTL |
| `CACHEFORGE_CACHE_NAMESPACE` | `cacheforge` | Key prefix |
| `CACHEFORGE_CACHE_VERSION` | `v1` | Key schema version |
| `CACHEFORGE_CACHE_BYPASS` | `false` | If true, cache is disabled (benchmarking only) |
| `CACHEFORGE_CIRCUIT_FAILURE_THRESHOLD` | `5` | Failures before breaker opens |
| `CACHEFORGE_CIRCUIT_WINDOW_SIZE` | `20` | Sliding window size |
| `CACHEFORGE_CIRCUIT_RECOVERY_TIMEOUT_SECONDS` | `5.0` | Wait before half-open probe |

---

## API endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness |
| `GET` | `/health/ready` | Readiness (Redis + Postgres ping) |
| `GET` | `/products/{id}` | Cache-aside read |
| `PATCH` | `/products/{id}` | Update + invalidate |
| `DELETE` | `/products/{id}` | Delete + invalidate |
| `POST` | `/admin/cache/invalidate/{id}` | Manual invalidation |
| `POST` | `/admin/cache/warm?limit=N` | Warm the first N products |
| `GET` | `/admin/cache/circuit-state` | Current breaker state |
| `GET` | `/metrics/` | Prometheus metrics |

> **Note:** Admin endpoints are unprotected in this demo. Any non-local deployment must add auth (see spec §16).

---

## Consistency model

See **[CONSISTENCY.md](CONSISTENCY.md)** for the full model. Summary:

- PostgreSQL is the source of truth. Every write commits to Postgres first, then invalidates Redis.
- Under **invalidation failure** (Window 1), stale data serves until TTL expires (default 300s).
- Under **read/write races** (Window 2), a slow reader can repopulate the cache with a pre-write value; stale until TTL.
- **No distributed lock.** Phase 3 measured this trade-off explicitly: cross-process duplication is bounded by worker count, so the lock overhead is not justified.
- **TTL is the safety net** — invalidation is not guaranteed.

---

## Resilience

- **Per-op timeouts** on all Redis calls (default 500ms)
- **Single-flight fallback** — a burst of `CacheUnavailable` coalesces to one DB query
- **Circuit breaker** — after N failures in the sliding window, all Redis ops short-circuit for `recovery_timeout`, then probe half-open
- **Bounded warming** — batched concurrency, capped retries, per-key backoff

The breaker exists because *fail-open does not mean unlimited database traffic* (spec §8.1).

---

## Benchmarks

See **[benchmarks/README.md](benchmarks/README.md)** for full results, methodology, and environment. Highlights:

| Phase | What | Result |
|---|---|---|
| Phase 3 | Cross-process stampede | `pg_delta=2` for 500 concurrent requests across 2 workers → distributed lock **not** justified |
| Phase 4 | Cold vs warm cache | 100/100 hits when warm, **2.69× speedup**, 0 read-path DB queries |
| Phase 5a | Circuit breaker impact | `pg_delta` unchanged, `loader_delta` → 0 (fallback served the burst), elapsed unchanged on WSL2 |

Every run is reproducible with `python -m benchmarks.<phase>`. Every claim in this README has a corresponding measurement.

---

## Known limitations

1. **WSL2 networking dominates concurrent-burst latency.** 500-connection bursts take ~7s regardless of cache behavior. Phase 7 moves the load generator into the Docker network.
2. **Postgres stats undercount on WSL2.** Cold-path `pg_delta` is a lower bound; warm-path `pg_delta` requires a canary-settle (implemented). Not a measurement bug — an environment constraint, documented.
3. **List/query caches are not cached.** Only detail views. Rationale and future strategy in `CONSISTENCY.md`.
4. **Negative caching is not enabled.** Missing-product lookups hit the DB every time.
5. **Transactional outbox is not yet implemented** (Phase 6). Invalidation failure currently means stale-until-TTL.
6. **Admin endpoints are unauthenticated.** Local demo only.
7. **No Grafana dashboard yet** (Phase 7).

---

## Roadmap

- [x] **Phase 1** — vertical slice: cache-aside, invalidation, metrics
- [x] **Phase 1.1** — real-Redis integration tests
- [x] **Phase 2** — race tests + `CONSISTENCY.md` + invalidation-failure test
- [x] **Phase 3** — stampede measurement (no lock needed) + bounded fallback
- [x] **Phase 4** — cache warming worker + cold/warm benchmark
- [x] **Phase 5a** — circuit breaker wired into `Cache`
- [x] **Phase 5b** — container-level outage/recovery test
- [x] **Phase 5c** — Postgres slowdown + retry-storm tests
- [x] **Phase 6** — transactional outbox for reliable invalidation
- [x] **Phase 7** — k6 load tests, Grafana dashboard, throughput experiments
- [ ] **Phase 8** — CI, Dockerfile, release quality

---

## Stack

Python 3.12+ · FastAPI · Redis 7 · PostgreSQL 16 · SQLAlchemy 2 · asyncpg · Pydantic · prometheus-client · pytest · httpx · Docker Compose · ruff

---

## License

MIT. See [LICENSE](LICENSE).

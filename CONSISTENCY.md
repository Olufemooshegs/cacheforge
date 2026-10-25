# CacheForge Consistency Model

> **One-line version:** PostgreSQL is truth. Redis is a performance layer. Under failure, reads can be stale for up to `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS`.

## Guarantees

### Write path

Every mutating operation follows this sequence:

1. `UPDATE products SET ...` — executed against PostgreSQL.
2. PostgreSQL commit acknowledged.
3. `DEL cacheforge:v1:product:<id>` — sent to Redis.

**Success guarantee:** after step 3 completes, the next read of
`cacheforge:v1:product:<id>` misses the cache and reads PostgreSQL.

### Read path (cache-aside)

1. `GET cacheforge:v1:product:<id>` from Redis.
2. Hit → deserialize, return.
3. Miss → `SELECT` from PostgreSQL, then
   `SET cacheforge:v1:product:<id> <json> EX <ttl>`.

**Success guarantee:** two consecutive reads of the same key with no
write in between cause at most one PostgreSQL query per process
(single-flight) and one cache fill.

## Known stale windows

### Window 1 — invalidation failure

If step 3 of the write path fails (Redis unavailable or timed out),
the invalidation is dropped. The cache serves the pre-write value
until TTL expires.

| | |
|---|---|
| **Bound** | up to `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS` (300 s dev) |
| **Test** | `tests/failure_injection/test_invalidation_failure.py` |
| **Mitigation** | Phase 6 transactional outbox: invalidation event written in the same DB transaction as the business update, delivered by a background worker with retries. Window shrinks to outbox processing latency (typically sub-second). |

### Window 2 — stale-read race

A reader that misses the cache can fetch a value from PostgreSQL,
pause (GC, scheduler, network), then write that value to Redis *after*
a concurrent write has already invalidated the key. The stale value
then persists until TTL.
T=0.0 Reader: GET key -> miss
T=0.1 Reader: SELECT -> price=1739
T=0.2 Reader: (paused before SET)
T=0.3 Writer: UPDATE -> price=99999
T=0.4 Writer: COMMIT
T=0.5 Writer: DEL key -> no-op (key was absent)
T=0.6 Reader: (resumes) SET key price=1739 <-- stale
T=0.7+ Read: GET key -> hit, price=1739 <-- stale


| | |
|---|---|
| **Bound** | up to `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS` from the reader's `SET` completion |
| **Test** | `tests/concurrency/test_stale_read_race.py` |
| **Status** | **Accepted.** Mitigation deferred to Phase 3+, contingent on Phase 7 benchmark evidence. |

Mitigation options considered (none free):

- **Version-checked write** (`SET IF version > stored`): needs Lua or
  `WATCH/MULTI`. Adds latency on every read; blocks on hot keys.
- **Single-flight + revalidate-on-write**: reduces probability, not the
  window. Still races.
- **Versioned keys** (`product:v3:42` where `v3` is bumped on write): readers
  write to the obsolete version's key, so stale writes are harmless. Costs
  an extra Redis round-trip per read; requires a version store; cleanup of
  old versions is nontrivial.
- **Distributed lock around read+write**: serializes all readers on a hot
  key. Defeats the point of caching.

The right fix depends on the observed race frequency, which depends on
TTL, request rate, and DB query latency. Phase 7 benchmarks will decide.

## Out of scope for v0.1

### List/query caches

Only detail views (`cacheforge:v1:product:<id>`) are cached. Paginated list
views (`/products?category=books&page=1`) are **not** cached.

Rationale:

- No evidence in current benchmarks that list queries dominate DB load.
- Invalidating N paginated list keys per write requires one of:
  - `SCAN cacheforge:v1:product-list:*` — violates spec §5.4
    ("Avoid broad key scans in request paths").
  - **Versioned category keys** — `cacheforge:v1:category:books:version`
    bumped on write; list keys embed the version number. Correct, but adds
    a per-read round-trip and unbounded version growth.
- Spec §19: "Begin with the smallest working implementation."

When benchmark evidence justifies list caching, versioned category keys
with targeted tests will be added.

### Negative caching

`None` results (missing products) are not cached. Every request for a
nonexistent product hits PostgreSQL.

Rationale: spec §5.2 says negative caching "should use a shorter TTL and
be configurable." Deferred until we observe a workload where
miss-driven negative lookups dominate.

## Operational implications

- **Read-your-own-writes is not guaranteed** across connections. A client
  that writes and immediately reads via a different connection may see
  the pre-write value if invalidation hasn't propagated, or if Window 2
  occurs. Applications needing RYOW must either read directly from
  PostgreSQL or use a stronger mechanism (not in scope for v0.1).
- **TTL is the safety net.** Every entry expires eventually. Never rely
  on invalidation alone for correctness of critical reads without a
  version check.
- **DB writes that bypass `ProductService` bypass invalidation.** All
  writes must go through the service for cache to stay consistent.
  Enforced by convention, not by schema. A future phase could enforce
  this via `LISTEN/NOTIFY` or a DB trigger that inserts outbox events.
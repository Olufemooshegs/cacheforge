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

  ## Stale-serving policy (Phase 5)

Spec §8.2 asks: when PostgreSQL is unavailable but Redis holds a value,
may cached reads still serve? Possible policies:

| Policy | Behavior | CacheForge |
|---|---|---|
| Serve valid cached values for read-only | Hits OK, misses fail | **Chosen** |
| Serve stale values for approved data types | Allowlist required | Not implemented |
| Reject authoritative reads | All fails | Not chosen |
| Never claim freshness without DB confirmation | Always fail on DB down | Not chosen |

### Our policy

**Cache hits are served during a Postgres outage. Cache misses fail with
HTTP 500.**

Rationale:

1. The value was already served to a user before the outage started.
   Serving it again within its TTL is no less fresh than serving it
   would have been without the outage.
2. Cache misses **cannot** be served because we have no source of truth.
   We do not synthesize data, and we do not serve a "best guess."
3. Writes always require PostgreSQL, so they fail cleanly during an
   outage. There is no "accepted but not applied" state.

### What this means operationally

| Request | Postgres OK | Postgres slow | Postgres down |
|---|---|---|---|
| Cache hit | Served | Served (fast) | **Served (fast)** |
| Cache miss | DB load, served | DB load, slow | **HTTP 500** |
| Write (PATCH/DELETE) | Committed | Slow commit | **HTTP 500** |
| `/health/ready` | 200 | 200 | **503** |

### What we do NOT do

- **No stale-while-revalidate.** There is no background refresh on
  read; there is no "stale" window that serves past TTL. Every read
  either hits within TTL or loads from Postgres.
- **No negative caching.** Missing rows hit Postgres every time; during
  a DB outage, missing-ID lookups fail 500 with no fallback.
- **No "assume Redis is authoritative" mode.** Redis never overrides
  Postgres; if the two disagree, Postgres wins on the next miss.

### TTL bounds the exposure

If Postgres dies while a key is cached:

- Reads serve the cached value for up to `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS`
  (300s default), then fail.
- After TTL expires, `/health/ready` is 503 and cache misses fail 500.

This means the outward-visible behavior under DB outage is: **degraded,
not silently wrong**. Users see stale reads (bounded by TTL) and then
clean errors, never "new-looking data that is actually stale."

### Writes and staleness

Writes during a Postgres outage fail at the commit step. Nothing is
written. There is no queued-write-that-replays-later mode (that would
be eventual consistency of writes, out of scope for v0.1). Phase 6
transactional outbox is about *invalidation delivery*, not about
buffering writes.

## Window 1 — now closed (Phase 6b)

Before Phase 6b, an update that committed to PostgreSQL but then failed
to invalidate Redis left the cache serving the pre-write value until TTL
(up to `CACHEFORGE_CACHE_DEFAULT_TTL_SECONDS`, default 300s).

Phase 6b closes this with a transactional outbox:

### Write path (Phase 6b)

1. `UPDATE products SET ...` — business change.
2. `INSERT INTO outbox_events(topic, payload)` — invalidation intent.
3. **COMMIT** — both rows land atomically.
4. Best-effort `DEL cacheforge:v1:product:<id>`.
5. If (4) succeeds, mark the outbox row `processed_at = now()`.
6. If (4) fails (`CacheUnavailable` or breaker OPEN), leave the row
   unprocessed. A background worker delivers it on the next poll cycle.

### What this changes

| Scenario | Before 6b | After 6b |
|---|---|---|
| Redis up | Invalidate in request path | Invalidate in request path (same) |
| Redis briefly down | Stale until TTL | Stale until worker delivers (typically <200ms) |
| Redis down for hours | Stale until TTL | Stale until TTL, but only if Redis is down for the full TTL |
| App crashes after commit | Stale until TTL | Outbox row survives; new worker picks it up on next start |

### What this does not change

- **Read-your-own-writes** is still not guaranteed across connections.
  The direct invalidation is best-effort and async relative to the
  client's `PATCH` response.
- **Window 2** (stale read races) is unaffected. A slow reader can
  still repopulate the cache with a pre-write value; only the write
  path got durability, not the read path.
- **Writes never buffer.** A `PATCH` during a Postgres outage still
  fails cleanly. The outbox is about *invalidation delivery*, not about
  making writes durable against DB outage.

### Guarantees

- **Durability**: once `PATCH` returns 200, the invalidation intent is
  committed and will eventually be delivered, even if the app crashes
  between the commit and the Redis call.
- **Bounded delay**: on the happy path, sub-millisecond (direct call).
  On `CacheUnavailable`, bounded by the worker poll interval (default
  100ms) + Redis recovery time.
- **Idempotent**: `DEL` is idempotent; reprocessing after a crash is a
  no-op. Malformed payloads are marked processed rather than retried
  forever.

### Tests

- `tests/integration/test_outbox.py` — 6 tests: event written with
  update, deferred when Redis down, worker delivers deferred event,
  idempotent on reprocess, handles malformed payload.
- `tests/failure_injection/test_outbox_window_closure.py` — the
  load-bearing test that closes Window 1 end-to-end.
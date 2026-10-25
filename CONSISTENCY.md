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

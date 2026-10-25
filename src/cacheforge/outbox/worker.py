"""Polling worker for the transactional outbox.

Reads unprocessed rows from `outbox_events`, delivers invalidations to
Redis, and marks each row processed.

Design choices:

  - Uses `SELECT ... FOR UPDATE SKIP LOCKED`. Multiple workers on the
    same DB will not fight over the same rows — each picks a disjoint
    batch. Safe for multi-worker uvicorn.
  - Delivery is idempotent: `cache.delete(key)` is a no-op if the key is
    already gone, so reprocessing after a crash is safe.
  - On `CacheUnavailable` (Redis down or breaker open), the row is NOT
    marked processed. The next poll retries. Bounded by TTL as the
    safety net — if Redis never returns, the cache eventually expires.
  - Malformed payloads are marked processed (with a metric) rather than
    retried forever — they will never succeed.
  - Poll interval is configurable; default 100ms. Bounds the "recently
    committed but not yet invalidated" window when the direct-invalidate
    fast path fails.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..client import Cache
from ..exceptions import CacheUnavailable
from ..observability import metrics

logger = logging.getLogger(__name__)


class OutboxWorker:
    def __init__(
        self,
        *,
        sessionmaker: async_sessionmaker[AsyncSession],
        cache: Cache,
        poll_interval: float = 0.1,
        batch_size: int = 50,
        error_backoff: float = 1.0,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be > 0")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        self._sessionmaker = sessionmaker
        self._cache = cache
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._error_backoff = error_backoff
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Signal run_forever to exit at the next loop tick."""
        self._stopped.set()

    async def run_forever(self) -> None:
        """Main loop. Cancel or call `stop()` to exit."""
        logger.info(
            "outbox worker started",
            extra={"poll_interval": self._poll_interval, "batch_size": self._batch_size},
        )
        try:
            while not self._stopped.is_set():
                try:
                    processed = await self.process_batch()
                    if processed == 0:
                        await asyncio.sleep(self._poll_interval)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "outbox worker error, backing off",
                        extra={"error": f"{type(exc).__name__}: {exc}"},
                    )
                    await asyncio.sleep(self._error_backoff)
        except asyncio.CancelledError:
            logger.info("outbox worker cancelled")
            raise
        finally:
            logger.info("outbox worker stopped")

    async def process_batch(self) -> int:
        """Process one batch. Return number of rows successfully delivered."""
        async with self._sessionmaker() as session, session.begin():
            pending = (
                await session.execute(
                    select(
                        _outbox_table(session).c.id,
                        _outbox_table(session).c.payload,
                        _outbox_table(session).c.created_at,
                    )
                    .where(_outbox_table(session).c.processed_at.is_(None))
                    .order_by(_outbox_table(session).c.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()

            depth_now = len(pending)
            metrics.OUTBOX_QUEUE_DEPTH.set(depth_now)

            if not pending:
                return 0

            succeeded = 0
            now_utc = datetime.now(UTC)

            for row in pending:
                try:
                    payload = json.loads(row.payload)
                    key = payload["key"]
                    if not isinstance(key, str) or not key:
                        raise KeyError("key must be a non-empty string")
                except (json.JSONDecodeError, KeyError, TypeError) as exc:
                    logger.error(
                        "outbox malformed payload, marking processed",
                        extra={"id": row.id, "error": f"{type(exc).__name__}: {exc}"},
                    )
                    metrics.OUTBOX_FAILED.labels(reason="malformed").inc()
                    await session.execute(
                        update(_outbox_table(session))
                        .where(_outbox_table(session).c.id == row.id)
                        .values(processed_at=func.now())
                    )
                    continue

                try:
                    await self._cache.delete(key)
                except CacheUnavailable:
                    # Redis unreachable. Leave unprocessed; retry next cycle.
                    metrics.OUTBOX_FAILED.labels(reason="cache_unavailable").inc()
                    logger.warning(
                        "outbox delivery deferred, cache unavailable",
                        extra={"id": row.id},
                    )
                    continue

                await session.execute(
                    update(_outbox_table(session))
                    .where(_outbox_table(session).c.id == row.id)
                    .values(processed_at=func.now())
                )
                succeeded += 1
                metrics.OUTBOX_PROCESSED.inc()
                delay = (now_utc - row.created_at).total_seconds()
                metrics.OUTBOX_DELAY.observe(delay)
                metrics.CACHE_INVALIDATIONS.labels("ok", "outbox").inc()

            return succeeded


def _outbox_table(session: AsyncSession) -> Any:
    """Resolve the OutboxEvent table from the session's metadata.

    Done lazily so the worker package has no import-time dependency on
    the demo models. Uses the metadata that the sessionmaker's engine
    was built against.
    """
    from demo.models import OutboxEvent

    _ = session  # ensure call signature is stable
    return OutboxEvent.__table__

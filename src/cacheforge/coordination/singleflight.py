from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class SingleFlight:
    """Per-process request coalescing.

    Concurrent callers awaiting the same key share one in-flight task. If a
    caller is cancelled, the task continues for the others via asyncio.shield.
    """

    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Task[object]] = {}
        self._lock = asyncio.Lock()

    async def do(self, key: str, factory: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(factory())
                self._inflight[key] = task
                task.add_done_callback(lambda t, k=key: self._release(k, t))  # type: ignore[arg-type]
        return await asyncio.shield(task)  # type: ignore[arg-type]

    def _release(self, key: str, task: asyncio.Task[object]) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]
        # Retrieve exception to suppress "never retrieved" warnings when no waiter awaited.
        if not task.cancelled():
            task.exception()

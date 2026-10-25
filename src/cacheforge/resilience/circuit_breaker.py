"""A minimal circuit breaker for Redis operations.

Spec §8.4: "Keep the implementation simple and test state transitions.
A circuit breaker is not a substitute for timeouts."

States:

    CLOSED      -> normal operation; failures are counted
    OPEN        -> all calls fail fast for `recovery_timeout` seconds
    HALF_OPEN   -> after recovery_timeout, allow one probe; if it
                   succeeds, close; if it fails, reopen for another
                   recovery_timeout

Design choices:
  - Sliding window is *count-based* (last N calls), not time-based.
    Simpler, deterministic for tests, sufficient for this workload.
  - The half-open probe is single-flight: concurrent calls during
    HALF_OPEN all fail fast. Only the winner of the lock gets to try.
  - Injectable clock so tests can advance time without sleeping.
  - State transitions emit metrics via `cache_circuit_state` gauge.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import TypeVar

from prometheus_client import Gauge

logger = logging.getLogger(__name__)

CIRCUIT_STATE_GAUGE = Gauge(
    "cache_circuit_state",
    "Circuit breaker state (0=closed, 1=half_open, 2=open)",
    ["name"],
)

T = TypeVar("T")


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    @property
    def gauge_value(self) -> int:
        return {"closed": 0, "half_open": 1, "open": 2}[self.value]


class CircuitOpen(Exception):
    """Raised when the breaker is OPEN and short-circuits a call."""


class CircuitBreaker:
    def __init__(
        self,
        *,
        name: str,
        failure_threshold: int = 5,
        window_size: int = 10,
        recovery_timeout: float = 5.0,
        half_open_max_calls: int = 1,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1 or window_size < 1:
            raise ValueError("failure_threshold and window_size must be >= 1")
        if failure_threshold > window_size:
            raise ValueError("failure_threshold cannot exceed window_size")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")

        self._name = name
        self._failure_threshold = failure_threshold
        self._window_size = window_size
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._clock = clock

        self._state = CircuitState.CLOSED
        self._failures: deque[bool] = deque(maxlen=window_size)
        self._opened_at: float = 0.0
        self._half_open_in_flight = 0
        self._lock = asyncio.Lock()

        CIRCUIT_STATE_GAUGE.labels(name=self._name).set(self._state.gauge_value)

    @property
    def state(self) -> CircuitState:
        return self._state

    async def call(self, func: Callable[[], Awaitable[T]]) -> T:
        """Execute `func` through the breaker.

        Raises `CircuitOpen` if the breaker is OPEN (or HALF_OPEN with
        the probe slot already taken). Any exception raised by `func`
        counts as a failure; success resets the window.
        """
        self._maybe_transition_out_of_open()

        if self._state == CircuitState.OPEN:
            raise CircuitOpen(f"circuit {self._name!r} is OPEN")

        if self._state == CircuitState.HALF_OPEN:
            async with self._lock:
                if self._half_open_in_flight >= self._half_open_max_calls:
                    raise CircuitOpen(f"circuit {self._name!r} HALF_OPEN probe in flight")
                self._half_open_in_flight += 1
            try:
                result = await func()
            except Exception:
                self._transition_to_open()
                raise
            else:
                self._transition_to_closed()
                return result
            finally:
                async with self._lock:
                    self._half_open_in_flight -= 1

        # CLOSED
        try:
            result = await func()
        except Exception:
            self._record_failure()
            raise
        else:
            self._record_success()
            return result

    def _record_failure(self) -> None:
        self._failures.append(True)
        if sum(self._failures) >= self._failure_threshold:
            self._transition_to_open()

    def _record_success(self) -> None:
        self._failures.append(False)

    def _maybe_transition_out_of_open(self) -> None:
        if self._state != CircuitState.OPEN:
            return
        if self._clock() - self._opened_at >= self._recovery_timeout:
            self._set_state(CircuitState.HALF_OPEN)

    def _transition_to_open(self) -> None:
        self._opened_at = self._clock()
        self._set_state(CircuitState.OPEN)

    def _transition_to_closed(self) -> None:
        self._failures.clear()
        self._set_state(CircuitState.CLOSED)

    def _set_state(self, new_state: CircuitState) -> None:
        if new_state == self._state:
            return
        logger.info(
            "circuit breaker state change",
            extra={
                "circuit": self._name,
                "from": self._state.value,
                "to": new_state.value,
            },
        )
        self._state = new_state
        CIRCUIT_STATE_GAUGE.labels(name=self._name).set(new_state.gauge_value)

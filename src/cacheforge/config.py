from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CacheConfig:
    namespace: str = "cacheforge"
    version: str = "v1"
    default_ttl_seconds: int = 300
    max_ttl_seconds: int = 3600
    ttl_jitter_seconds: int = 0
    redis_timeout_seconds: float = 0.5
    cache_bypass: bool = False

from __future__ import annotations

import hashlib
import json
from typing import Any


class KeyBuilder:
    """Deterministic, namespace-versioned cache keys.

    Example: KeyBuilder("cacheforge", "v1").build("product", 123)
             -> "cacheforge:v1:product:123"
    """

    def __init__(self, namespace: str, version: str = "v1", separator: str = ":") -> None:
        if not namespace:
            raise ValueError("namespace must be non-empty")
        if separator in namespace or separator in version:
            raise ValueError(f"separator {separator!r} cannot appear in namespace/version")
        self._ns = namespace
        self._ver = version
        self._sep = separator

    def build(self, *parts: Any) -> str:
        return self._sep.join([self._ns, self._ver, *(self._normalize(p) for p in parts)])

    def hashed(self, *parts: Any) -> str:
        payload = json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        return self._sep.join([self._ns, self._ver, "h", digest])

    @staticmethod
    def _normalize(part: Any) -> str:
        if isinstance(part, str):
            if not part:
                raise ValueError("key segment must be non-empty")
            return part
        if isinstance(part, bool):
            return "true" if part else "false"
        if isinstance(part, (int, float)):
            return str(part)
        if part is None:
            return "null"
        return json.dumps(part, sort_keys=True, separators=(",", ":"), default=str)

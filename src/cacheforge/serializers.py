from __future__ import annotations

import json
from typing import Any, Protocol

from .exceptions import CacheSerializationError


class Serializer(Protocol):
    def dumps(self, value: Any) -> bytes: ...
    def loads(self, raw: bytes) -> Any: ...


class JsonSerializer:
    def __init__(self) -> None:
        self._sep = (",", ":")

    def dumps(self, value: Any) -> bytes:
        try:
            return json.dumps(
                value, separators=self._sep, ensure_ascii=False, default=_json_default
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise CacheSerializationError(f"encode failed: {exc}") from exc

    def loads(self, raw: bytes) -> Any:
        try:
            return json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise CacheSerializationError(f"decode failed: {exc}") from exc


def _json_default(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"{type(obj).__name__} is not JSON-serializable")

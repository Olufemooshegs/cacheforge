class CacheError(Exception):
    """Base for all CacheForge errors."""


class CacheUnavailable(CacheError):
    """Redis is unreachable or timed out. Distinct from a miss."""


class CacheSerializationError(CacheError):
    """Failed to encode or decode a cached value."""

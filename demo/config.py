from pydantic_settings import BaseSettings, SettingsConfigDict


class DemoSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="CACHEFORGE_", extra="ignore")

    database_url: str = "postgresql+asyncpg://cacheforge:cacheforge@localhost:5433/cacheforge"
    redis_url: str = "redis://localhost:6380/0"
    cache_default_ttl_seconds: int = 300
    cache_namespace: str = "cacheforge"
    cache_version: str = "v1"
    cache_bypass: bool = False

    circuit_failure_threshold: int = 5
    circuit_window_size: int = 20
    circuit_recovery_timeout_seconds: float = 5.0

    # Test-only: inject artificial latency into DB loads. 0.0 disables.
    # Used by benchmarks/phase5c_postgres_resilience.py to simulate a slow
    # database without external tooling.
    query_delay_seconds: float = 0.0

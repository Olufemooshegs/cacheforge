from pydantic_settings import BaseSettings, SettingsConfigDict


class DemoSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="CACHEFORGE_", extra="ignore")

    database_url: str = "postgresql+asyncpg://cacheforge:cacheforge@localhost:5432/cacheforge"
    redis_url: str = "redis://localhost:6379/0"
    cache_default_ttl_seconds: int = 300
    cache_namespace: str = "cacheforge"
    cache_version: str = "v1"
    cache_bypass: bool = False

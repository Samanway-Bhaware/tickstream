"""Application configuration via pydantic-settings.

Sources (highest → lowest priority):
1. Environment variables (prefixed ``TICKSTREAM_``)
2. ``config.toml`` in the working directory (optional)
3. Defaults defined here
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_CONFIG_TOML = Path("config.toml")


class Settings(BaseSettings):
    """Runtime configuration for tickstream."""

    model_config = SettingsConfigDict(
        env_prefix="TICKSTREAM_",
        env_file=".env",
        env_file_encoding="utf-8",
        # Load config.toml when it exists.
        toml_file=str(_CONFIG_TOML) if _CONFIG_TOML.exists() else None,
        extra="ignore",
    )

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["json", "console"] = Field(
        default="console",
        description="'json' for production, 'console' for development",
    )

    # Storage
    db_url: str = Field(
        default="postgresql://localhost:5432/tickstream",
        description="SQLAlchemy-compatible database URL",
    )
    db_pool_size: int = Field(default=10, ge=1, le=200)

    # Ingestor
    ingestor_buffer_size: int = Field(
        default=10_000,
        ge=1,
        description="Number of ticks to buffer before flushing to storage",
    )
    ingestor_flush_interval_ms: int = Field(
        default=500,
        ge=10,
        description="Maximum time between flushes in milliseconds",
    )

    # Monitoring
    metrics_port: int = Field(default=9090, ge=1024, le=65535)
    metrics_enabled: bool = True

    @field_validator("log_level", mode="before")
    @classmethod
    def normalise_log_level(cls, v: object) -> str:
        return str(v).upper()


def get_settings() -> Settings:
    """Return a cached Settings instance (call once at startup)."""
    return Settings()

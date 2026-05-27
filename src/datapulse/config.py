"""Runtime configuration helpers for DataPulse."""

from __future__ import annotations

from dataclasses import dataclass
from os import environ
from typing import Mapping


@dataclass(frozen=True)
class Settings:
    """Configuration values loaded from environment variables."""

    storage_backend: str
    mysql_host: str
    mysql_port: int
    mysql_database: str
    mysql_user: str
    mysql_password: str


def load_settings(environment: Mapping[str, str] | None = None) -> Settings:
    """Load DataPulse settings from a mapping or the process environment."""
    source = environment
    if source is None:
        source = environ

    # Keep local development defaults explicit and easy to override.
    return Settings(
        storage_backend=source.get("STORAGE_BACKEND", "mysql"),
        mysql_host=source.get("MYSQL_HOST", "127.0.0.1"),
        mysql_port=int(source.get("MYSQL_PORT", "3306")),
        mysql_database=source.get("MYSQL_DATABASE", "datapulse"),
        mysql_user=source.get("MYSQL_USER", "datapulse"),
        mysql_password=source.get("MYSQL_PASSWORD", "datapulse"),
    )

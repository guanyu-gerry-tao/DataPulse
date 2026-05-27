"""Shared data models for storage adapters."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone


ALLOWED_JOB_STATUSES = frozenset(
    {
        "QUEUED",
        "PROCESSING",
        "SUCCEEDED",
        "FAILED",
        "DEAD_LETTERED",
    }
)


def utc_now() -> datetime:
    """Return the current UTC time for new records."""
    return datetime.now(timezone.utc)


def validate_job_status(status: str) -> None:
    """Raise ValueError when a job status is outside the known lifecycle."""
    if status not in ALLOWED_JOB_STATUSES:
        raise ValueError(f"Invalid job status: {status}")


@dataclass(frozen=True)
class JobRecord:
    """Metadata for one DataPulse processing job."""

    job_id: str
    status: str
    source_bucket: str | None = None
    source_key: str | None = None
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    attempt_count: int = 0
    last_error: str | None = None
    next_attempt_at: datetime | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        """Validate required fields after dataclass initialization."""
        if not self.job_id.strip():
            raise ValueError("job_id is required")

        # Keep status validation close to the model shared by all adapters.
        validate_job_status(self.status)


@dataclass(frozen=True)
class FileManifest:
    """Metadata that identifies one uploaded source file."""

    manifest_id: str
    job_id: str
    bucket: str
    object_key: str
    object_key_hash: str
    checksum: str | None = None
    content_type: str | None = None
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        """Validate required file manifest fields."""
        if not self.manifest_id.strip():
            raise ValueError("manifest_id is required")

        if not self.job_id.strip():
            raise ValueError("job_id is required")

        if not self.bucket.strip():
            raise ValueError("bucket is required")

        if not self.object_key.strip():
            raise ValueError("object_key is required")

        if not self.object_key_hash.strip():
            raise ValueError("object_key_hash is required")

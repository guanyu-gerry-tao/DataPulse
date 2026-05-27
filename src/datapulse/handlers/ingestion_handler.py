"""Local ingestion handler for S3 ObjectCreated-style events."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from uuid import uuid4

from datapulse.events.s3_event import parse_s3_object_created_event
from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.models import utc_now
from datapulse.queue.base import ProcessingMessage
from datapulse.queue.base import QueueAdapter
from datapulse.storage.base import StorageBackend


SUPPORTED_FILE_EXTENSIONS = frozenset({".csv", ".json"})


@dataclass(frozen=True)
class IngestionResult:
    """Result returned after handling one ingestion event."""

    job_id: str
    object_key_hash: str
    created: bool


def handle_s3_object_created_event(
    event: dict[str, object],
    storage: StorageBackend,
    queue: QueueAdapter,
    job_id_factory: Callable[[], str] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> IngestionResult:
    """Create a queued processing job from one local S3-style event."""
    parsed_event = parse_s3_object_created_event(event)
    _validate_supported_file(parsed_event.object_key)

    existing_job = storage.find_job_by_file(
        parsed_event.bucket,
        parsed_event.object_key_hash,
    )
    if existing_job is not None:
        return IngestionResult(
            job_id=existing_job.job_id,
            object_key_hash=parsed_event.object_key_hash,
            created=False,
        )

    if job_id_factory is None:
        job_id_factory = _default_job_id

    if clock is None:
        clock = utc_now

    now = clock()
    job_id = job_id_factory()

    # Create the job and manifest before publishing the local queue message.
    job = storage.create_job(
        JobRecord(
            job_id=job_id,
            status="QUEUED",
            source_bucket=parsed_event.bucket,
            source_key=parsed_event.object_key,
            created_at=now,
            updated_at=now,
        )
    )
    storage.record_file_manifest(
        FileManifest(
            manifest_id=f"manifest_{job.job_id}",
            job_id=job.job_id,
            bucket=parsed_event.bucket,
            object_key=parsed_event.object_key,
            object_key_hash=parsed_event.object_key_hash,
            checksum=parsed_event.etag,
            content_type=_content_type_for_key(parsed_event.object_key),
            created_at=now,
        )
    )
    queue.enqueue(
        ProcessingMessage(
            job_id=job.job_id,
            bucket=parsed_event.bucket,
            object_key=parsed_event.object_key,
            object_key_hash=parsed_event.object_key_hash,
        )
    )

    return IngestionResult(
        job_id=job.job_id,
        object_key_hash=parsed_event.object_key_hash,
        created=True,
    )


def _validate_supported_file(object_key: str) -> None:
    """Raise ValueError when the uploaded file type is not supported in M2."""
    suffix = PurePosixPath(object_key).suffix.lower()
    if suffix not in SUPPORTED_FILE_EXTENSIONS:
        raise ValueError(f"Unsupported file extension: {suffix}")


def _content_type_for_key(object_key: str) -> str:
    """Return a simple content type from the uploaded file extension."""
    suffix = PurePosixPath(object_key).suffix.lower()
    if suffix == ".csv":
        return "text/csv"

    if suffix == ".json":
        return "application/json"

    return "application/octet-stream"


def _default_job_id() -> str:
    """Create a unique local job id."""
    return f"job_{uuid4().hex}"

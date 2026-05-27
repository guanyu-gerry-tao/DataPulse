from __future__ import annotations

import json
from datetime import datetime
from datetime import timezone
from pathlib import Path

import pytest

from datapulse.handlers.ingestion_handler import handle_s3_object_created_event
from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.queue.base import ProcessingMessage
from datapulse.queue.memory import InMemoryQueueAdapter
from datapulse.storage.base import StorageBackend
from datapulse.storage.mysql import MySQLStorageAdapter
from tests.storage.test_mysql_storage import FakeMySQLConnection


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"


class FakeStorageBackend(StorageBackend):
    """Storage fake that keeps jobs and manifests in memory for handler tests."""

    def __init__(self) -> None:
        self.jobs: dict[str, JobRecord] = {}
        self.manifests: dict[tuple[str, str], FileManifest] = {}
        self.created_job_count = 0
        self.recorded_manifest_count = 0

    def create_job(self, job: JobRecord) -> JobRecord:
        """Store one job by id."""
        self.jobs[job.job_id] = job
        self.created_job_count = self.created_job_count + 1
        return job

    def get_job(self, job_id: str) -> JobRecord | None:
        """Return one job by id."""
        return self.jobs.get(job_id)

    def update_job_status(
        self,
        job_id: str,
        status: str,
        metadata: dict[str, object] | None = None,
    ) -> JobRecord:
        """Update a job status for protocol compatibility."""
        existing_job = self.jobs[job_id]
        updated_job = JobRecord(
            job_id=existing_job.job_id,
            status=status,
            source_bucket=existing_job.source_bucket,
            source_key=existing_job.source_key,
            created_at=existing_job.created_at,
            updated_at=existing_job.updated_at,
        )
        self.jobs[job_id] = updated_job
        return updated_job

    def record_file_manifest(self, manifest: FileManifest) -> FileManifest:
        """Store one file manifest by idempotency key."""
        self.manifests[(manifest.bucket, manifest.object_key_hash)] = manifest
        self.recorded_manifest_count = self.recorded_manifest_count + 1
        return manifest

    def find_job_by_file(self, bucket: str, object_key_hash: str) -> JobRecord | None:
        """Return an existing job for one uploaded file."""
        manifest = self.manifests.get((bucket, object_key_hash))
        if manifest is None:
            return None

        return self.jobs[manifest.job_id]


def test_ingestion_handler_creates_job_manifest_and_processing_message() -> None:
    storage = FakeStorageBackend()
    queue = InMemoryQueueAdapter()
    event = json.loads((FIXTURE_DIR / "s3_object_created_event.json").read_text())
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)

    result = handle_s3_object_created_event(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_001",
        clock=lambda: now,
    )

    assert result.job_id == "job_001"
    assert result.created is True
    assert storage.created_job_count == 1
    assert storage.recorded_manifest_count == 1
    assert storage.jobs["job_001"].status == "QUEUED"
    assert storage.jobs["job_001"].source_bucket == "datapulse-local-raw"
    assert storage.jobs["job_001"].source_key == "uploads/orders sample.csv"
    assert queue.messages == [
        ProcessingMessage(
            job_id="job_001",
            bucket="datapulse-local-raw",
            object_key="uploads/orders sample.csv",
            object_key_hash=result.object_key_hash,
        )
    ]


def test_ingestion_handler_is_idempotent_for_repeated_file_event() -> None:
    storage = FakeStorageBackend()
    queue = InMemoryQueueAdapter()
    event = json.loads((FIXTURE_DIR / "s3_object_created_event.json").read_text())
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)

    first_result = handle_s3_object_created_event(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_001",
        clock=lambda: now,
    )
    second_result = handle_s3_object_created_event(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_002",
        clock=lambda: now,
    )

    assert first_result.job_id == "job_001"
    assert second_result.job_id == "job_001"
    assert second_result.created is False
    assert storage.created_job_count == 1
    assert storage.recorded_manifest_count == 1
    assert len(queue.messages) == 1


def test_ingestion_handler_idempotency_uses_mysql_manifest_lookup() -> None:
    connection = FakeMySQLConnection()
    storage = MySQLStorageAdapter(connection_factory=lambda: connection)
    queue = InMemoryQueueAdapter()
    event = json.loads((FIXTURE_DIR / "s3_object_created_event.json").read_text())
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)

    first_result = handle_s3_object_created_event(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_mysql_001",
        clock=lambda: now,
    )
    second_result = handle_s3_object_created_event(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_mysql_002",
        clock=lambda: now,
    )

    assert first_result.job_id == "job_mysql_001"
    assert second_result.job_id == "job_mysql_001"
    assert second_result.created is False
    assert len(connection.jobs) == 1
    assert len(connection.file_manifests) == 1
    assert queue.messages == [
        ProcessingMessage(
            job_id="job_mysql_001",
            bucket="datapulse-local-raw",
            object_key="uploads/orders sample.csv",
            object_key_hash=first_result.object_key_hash,
        )
    ]


def test_ingestion_handler_rejects_unsupported_file_extension() -> None:
    storage = FakeStorageBackend()
    queue = InMemoryQueueAdapter()
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "datapulse-local-raw"},
                    "object": {"key": "uploads/readme.txt", "size": 10, "eTag": "etag"},
                }
            }
        ]
    }

    with pytest.raises(ValueError, match="Unsupported file extension"):
        handle_s3_object_created_event(event, storage=storage, queue=queue)

    assert storage.created_job_count == 0
    assert queue.messages == []

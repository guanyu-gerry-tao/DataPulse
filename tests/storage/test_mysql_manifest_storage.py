from __future__ import annotations

from datetime import datetime
from datetime import timezone

from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.storage.mysql import MySQLStorageAdapter
from tests.storage.test_mysql_storage import FakeMySQLConnection


def test_record_file_manifest_then_find_job_by_file_returns_job() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    job = adapter.create_job(
        JobRecord(
            job_id="job_manifest_001",
            status="QUEUED",
            source_bucket="datapulse-local-raw",
            source_key="uploads/orders.csv",
            created_at=now,
            updated_at=now,
        )
    )

    manifest = adapter.record_file_manifest(
        FileManifest(
            manifest_id="manifest_001",
            job_id=job.job_id,
            bucket="datapulse-local-raw",
            object_key="uploads/orders.csv",
            object_key_hash="hash-orders",
            checksum="sample-etag-001",
            content_type="text/csv",
            created_at=now,
        )
    )
    loaded_job = adapter.find_job_by_file("datapulse-local-raw", "hash-orders")

    assert manifest.job_id == "job_manifest_001"
    assert loaded_job == job


def test_find_job_by_file_returns_none_for_missing_manifest() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)

    loaded_job = adapter.find_job_by_file("datapulse-local-raw", "missing-hash")

    assert loaded_job is None

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal
from pathlib import Path

from datapulse.handlers.processing_handler import handle_processing_message
from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.queue.base import ProcessingMessage
from datapulse.storage.mysql import MySQLStorageAdapter
from tests.storage.test_mysql_storage import FakeMySQLConnection


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def seed_job_and_manifest(
    adapter: MySQLStorageAdapter,
    *,
    job_id: str,
    object_key: str,
    object_key_hash: str,
    now: datetime,
) -> None:
    """Create the M2 job and manifest records needed by processor tests."""
    adapter.create_job(
        JobRecord(
            job_id=job_id,
            status="QUEUED",
            source_bucket="datapulse-local-raw",
            source_key=object_key,
            created_at=now,
            updated_at=now,
        )
    )
    adapter.record_file_manifest(
        FileManifest(
            manifest_id=f"manifest_{job_id}",
            job_id=job_id,
            bucket="datapulse-local-raw",
            object_key=object_key,
            object_key_hash=object_key_hash,
            created_at=now,
        )
    )


def test_processing_handler_processes_valid_dataset_and_writes_summary() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    dataset_path = FIXTURE_DIR / "orders_sample.csv"
    seed_job_and_manifest(
        adapter,
        job_id="job_process_success",
        object_key="uploads/orders_sample.csv",
        object_key_hash="hash-success",
        now=now,
    )

    result = handle_processing_message(
        ProcessingMessage(
            job_id="job_process_success",
            bucket="datapulse-local-raw",
            object_key="uploads/orders_sample.csv",
            object_key_hash="hash-success",
        ),
        storage=adapter,
        object_reader=lambda bucket, key: dataset_path.read_text(),
        clock=lambda: now,
    )

    updated_job = adapter.get_job("job_process_success")
    summary = adapter.get_result_summary("job_process_success")
    assert result.status == "SUCCEEDED"
    assert updated_job is not None
    assert updated_job.status == "SUCCEEDED"
    assert updated_job.total_records == 2
    assert updated_job.valid_records == 2
    assert updated_job.invalid_records == 0
    assert len(connection.processed_records) == 2
    assert summary is not None
    assert summary.total_records == 2
    assert summary.total_amount == Decimal("49.98")


def test_processing_handler_records_bad_rows_without_losing_status() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    dataset_path = FIXTURE_DIR / "orders_with_bad_row.csv"
    seed_job_and_manifest(
        adapter,
        job_id="job_process_partial",
        object_key="uploads/orders_with_bad_row.csv",
        object_key_hash="hash-partial",
        now=now,
    )

    result = handle_processing_message(
        ProcessingMessage(
            job_id="job_process_partial",
            bucket="datapulse-local-raw",
            object_key="uploads/orders_with_bad_row.csv",
            object_key_hash="hash-partial",
        ),
        storage=adapter,
        object_reader=lambda bucket, key: dataset_path.read_text(),
        clock=lambda: now,
    )

    updated_job = adapter.get_job("job_process_partial")
    errors = adapter.list_processing_errors("job_process_partial")
    assert result.status == "FAILED"
    assert updated_job is not None
    assert updated_job.status == "FAILED"
    assert updated_job.total_records == 3
    assert updated_job.valid_records == 2
    assert updated_job.invalid_records == 1
    assert len(connection.processed_records) == 2
    assert len(errors) == 1
    assert errors[0].error_code == "INVALID_AMOUNT"


def test_processing_handler_is_idempotent_for_repeated_message() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    dataset_path = FIXTURE_DIR / "orders_sample.csv"
    message = ProcessingMessage(
        job_id="job_process_duplicate",
        bucket="datapulse-local-raw",
        object_key="uploads/orders_sample.csv",
        object_key_hash="hash-duplicate",
    )
    seed_job_and_manifest(
        adapter,
        job_id=message.job_id,
        object_key=message.object_key,
        object_key_hash=message.object_key_hash,
        now=now,
    )

    first_result = handle_processing_message(
        message,
        storage=adapter,
        object_reader=lambda bucket, key: dataset_path.read_text(),
        clock=lambda: now,
    )
    second_result = handle_processing_message(
        message,
        storage=adapter,
        object_reader=lambda bucket, key: dataset_path.read_text(),
        clock=lambda: now,
    )

    assert first_result.status == "SUCCEEDED"
    assert second_result.status == "SUCCEEDED"
    assert second_result.already_processed is True
    assert len(connection.processed_records) == 2


def test_processing_handler_records_retry_metadata_before_dlq() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    message = ProcessingMessage(
        job_id="job_process_retry",
        bucket="datapulse-local-raw",
        object_key="uploads/missing.csv",
        object_key_hash="hash-retry",
    )
    seed_job_and_manifest(
        adapter,
        job_id=message.job_id,
        object_key=message.object_key,
        object_key_hash=message.object_key_hash,
        now=now,
    )

    result = handle_processing_message(
        message,
        storage=adapter,
        object_reader=lambda bucket, key: (_ for _ in ()).throw(FileNotFoundError(key)),
        max_attempts=3,
        clock=lambda: now,
    )

    updated_job = adapter.get_job("job_process_retry")
    assert result.status == "FAILED"
    assert updated_job is not None
    assert updated_job.status == "FAILED"
    assert updated_job.attempt_count == 1
    assert updated_job.last_error == "uploads/missing.csv"
    assert updated_job.next_attempt_at is not None
    assert len(connection.dead_letter_messages) == 0


def test_processing_handler_saves_dlq_after_retry_exhaustion() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    message = ProcessingMessage(
        job_id="job_process_dlq",
        bucket="datapulse-local-raw",
        object_key="uploads/missing.csv",
        object_key_hash="hash-dlq",
    )
    seed_job_and_manifest(
        adapter,
        job_id=message.job_id,
        object_key=message.object_key,
        object_key_hash=message.object_key_hash,
        now=now,
    )

    result = handle_processing_message(
        message,
        storage=adapter,
        object_reader=lambda bucket, key: (_ for _ in ()).throw(FileNotFoundError(key)),
        max_attempts=1,
        clock=lambda: now,
    )

    updated_job = adapter.get_job("job_process_dlq")
    assert result.status == "DEAD_LETTERED"
    assert updated_job is not None
    assert updated_job.status == "DEAD_LETTERED"
    assert updated_job.attempt_count == 1
    assert len(connection.dead_letter_messages) == 1

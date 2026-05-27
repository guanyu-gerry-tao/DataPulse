from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal

import pytest

from datapulse.models import DeadLetterMessage
from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.models import ProcessedRecord
from datapulse.models import ProcessingError
from datapulse.models import ResultSummary
from datapulse.storage.base import StorageBackend
from datapulse.storage.dynamodb import DynamoDBStorageAdapter
from datapulse.storage.dynamodb import InMemoryDynamoDBTable
from datapulse.storage.mysql import MySQLStorageAdapter
from tests.storage.test_mysql_storage import FakeMySQLConnection


def mysql_storage_factory() -> StorageBackend:
    """Create a MySQL storage adapter backed by the local fake connection."""
    connection = FakeMySQLConnection()
    return MySQLStorageAdapter(connection_factory=lambda: connection)


def dynamodb_storage_factory() -> StorageBackend:
    """Create a DynamoDB storage adapter backed by an in-memory DynamoDB table."""
    table = InMemoryDynamoDBTable()
    return DynamoDBStorageAdapter(table=table)


@pytest.fixture(params=[mysql_storage_factory, dynamodb_storage_factory])
def storage(request: pytest.FixtureRequest) -> StorageBackend:
    """Run each contract test against every supported storage backend."""
    factory = request.param
    return factory()


def test_contract_create_and_get_job(storage: StorageBackend) -> None:
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    job = JobRecord(
        job_id="job_contract_001",
        status="QUEUED",
        source_bucket="datapulse-local-raw",
        source_key="uploads/orders.csv",
        created_at=now,
        updated_at=now,
    )

    created_job = storage.create_job(job)
    loaded_job = storage.get_job(job.job_id)

    assert created_job == job
    assert loaded_job == job


def test_contract_get_missing_job_returns_none(storage: StorageBackend) -> None:
    assert storage.get_job("missing_job") is None


def test_contract_update_job_status_preserves_unspecified_metadata(
    storage: StorageBackend,
) -> None:
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    retry_at = datetime(2026, 5, 27, 12, 5, tzinfo=timezone.utc)
    later = datetime(2026, 5, 27, 12, 6, tzinfo=timezone.utc)
    storage.create_job(
        JobRecord(
            job_id="job_contract_update",
            status="FAILED",
            total_records=10,
            valid_records=8,
            invalid_records=2,
            attempt_count=2,
            last_error="temporary failure",
            next_attempt_at=retry_at,
            created_at=now,
            updated_at=now,
        )
    )

    updated_job = storage.update_job_status(
        "job_contract_update",
        "PROCESSING",
        metadata={"updated_at": later},
    )

    assert updated_job.status == "PROCESSING"
    assert updated_job.total_records == 10
    assert updated_job.valid_records == 8
    assert updated_job.invalid_records == 2
    assert updated_job.attempt_count == 2
    assert updated_job.last_error == "temporary failure"
    assert updated_job.next_attempt_at == retry_at
    assert updated_job.updated_at == later


def test_contract_record_manifest_and_find_job_by_file(storage: StorageBackend) -> None:
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    storage.create_job(
        JobRecord(
            job_id="job_contract_manifest",
            status="QUEUED",
            source_bucket="datapulse-local-raw",
            source_key="uploads/orders.csv",
            created_at=now,
            updated_at=now,
        )
    )
    manifest = FileManifest(
        manifest_id="manifest_contract",
        job_id="job_contract_manifest",
        bucket="datapulse-local-raw",
        object_key="uploads/orders.csv",
        object_key_hash="hash-contract",
        checksum="etag-contract",
        content_type="text/csv",
        created_at=now,
    )

    saved_manifest = storage.record_file_manifest(manifest)
    loaded_job = storage.find_job_by_file("datapulse-local-raw", "hash-contract")

    assert saved_manifest == manifest
    assert loaded_job is not None
    assert loaded_job.job_id == "job_contract_manifest"


def test_contract_find_job_by_file_returns_none_for_missing_manifest(
    storage: StorageBackend,
) -> None:
    loaded_job = storage.find_job_by_file("datapulse-local-raw", "missing-hash")

    assert loaded_job is None


def test_contract_save_processed_records_is_idempotent_by_job_and_row(
    storage: StorageBackend,
) -> None:
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    storage.create_job(
        JobRecord(
            job_id="job_contract_records",
            status="QUEUED",
            created_at=now,
            updated_at=now,
        )
    )
    records = [
        ProcessedRecord(
            record_id="job_contract_records:row:1",
            job_id="job_contract_records",
            row_number=1,
            record_type="order",
            amount=Decimal("19.99"),
            currency="USD",
            payload={"order_id": "order-001"},
            created_at=now,
        )
    ]

    storage.save_processed_records("job_contract_records", records)
    storage.save_processed_records("job_contract_records", records)

    summary = ResultSummary(
        job_id="job_contract_records",
        total_records=1,
        valid_records=1,
        invalid_records=0,
        total_amount=Decimal("19.99"),
        summary={"record_type": "order"},
        created_at=now,
        updated_at=now,
    )
    storage.save_result_summary(summary)

    assert storage.get_result_summary("job_contract_records") == summary


def test_contract_save_and_list_processing_errors(storage: StorageBackend) -> None:
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    storage.create_job(
        JobRecord(
            job_id="job_contract_errors",
            status="QUEUED",
            created_at=now,
            updated_at=now,
        )
    )
    error = ProcessingError(
        error_id="job_contract_errors:row:2:error:INVALID_AMOUNT",
        job_id="job_contract_errors",
        row_number=2,
        error_code="INVALID_AMOUNT",
        error_message="amount must be numeric",
        created_at=now,
    )

    storage.save_processing_error(error)
    storage.save_processing_error(error)

    errors = storage.list_processing_errors("job_contract_errors")
    assert errors == [error]


def test_contract_save_and_update_result_summary(storage: StorageBackend) -> None:
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    later = datetime(2026, 5, 27, 12, 5, tzinfo=timezone.utc)
    storage.create_job(
        JobRecord(
            job_id="job_contract_summary",
            status="QUEUED",
            created_at=now,
            updated_at=now,
        )
    )
    first_summary = ResultSummary(
        job_id="job_contract_summary",
        total_records=1,
        valid_records=1,
        invalid_records=0,
        total_amount=Decimal("19.99"),
        summary={"record_type": "order"},
        created_at=now,
        updated_at=now,
    )
    second_summary = ResultSummary(
        job_id="job_contract_summary",
        total_records=2,
        valid_records=2,
        invalid_records=0,
        total_amount=Decimal("49.98"),
        summary={"record_type": "order"},
        created_at=later,
        updated_at=later,
    )

    storage.save_result_summary(first_summary)
    storage.save_result_summary(second_summary)

    loaded_summary = storage.get_result_summary("job_contract_summary")
    assert loaded_summary is not None
    assert loaded_summary.job_id == second_summary.job_id
    assert loaded_summary.total_records == second_summary.total_records
    assert loaded_summary.valid_records == second_summary.valid_records
    assert loaded_summary.invalid_records == second_summary.invalid_records
    assert loaded_summary.total_amount == second_summary.total_amount
    assert loaded_summary.summary == second_summary.summary
    assert loaded_summary.created_at == now
    assert loaded_summary.updated_at == later


def test_contract_save_dead_letter_message(storage: StorageBackend) -> None:
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    message = DeadLetterMessage(
        message_id="dlq_contract_001",
        job_id="job_contract_dlq",
        source_queue="local-processing",
        payload={"job_id": "job_contract_dlq"},
        error_message="file not found",
        attempt_count=3,
        created_at=now,
    )

    storage.save_dead_letter_message(message)

    assert storage.get_job("job_contract_dlq") is None

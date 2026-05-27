from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal

from datapulse.models import DeadLetterMessage
from datapulse.models import JobRecord
from datapulse.models import ProcessedRecord
from datapulse.models import ProcessingError
from datapulse.models import ResultSummary
from datapulse.storage.mysql import MySQLStorageAdapter
from tests.storage.test_mysql_storage import FakeMySQLConnection


def test_save_processed_records_is_idempotent_by_job_and_row() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    adapter.create_job(JobRecord(job_id="job_records", status="QUEUED", created_at=now, updated_at=now))
    records = [
        ProcessedRecord(
            record_id="job_records:row:1",
            job_id="job_records",
            row_number=1,
            record_type="order",
            amount=Decimal("19.99"),
            currency="USD",
            payload={"order_id": "order-001"},
            created_at=now,
        )
    ]

    adapter.save_processed_records("job_records", records)
    adapter.save_processed_records("job_records", records)

    assert len(connection.processed_records) == 1


def test_save_and_list_processing_errors_by_job() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    adapter.create_job(JobRecord(job_id="job_errors", status="QUEUED", created_at=now, updated_at=now))

    adapter.save_processing_error(
        ProcessingError(
            error_id="error_001",
            job_id="job_errors",
            row_number=2,
            error_code="INVALID_AMOUNT",
            error_message="amount must be numeric",
            created_at=now,
        )
    )

    errors = adapter.list_processing_errors("job_errors")
    assert len(errors) == 1
    assert errors[0].error_code == "INVALID_AMOUNT"


def test_save_and_get_result_summary() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    adapter.create_job(JobRecord(job_id="job_summary", status="QUEUED", created_at=now, updated_at=now))

    adapter.save_result_summary(
        ResultSummary(
            job_id="job_summary",
            total_records=2,
            valid_records=2,
            invalid_records=0,
            total_amount=Decimal("49.98"),
            summary={"currency": "USD"},
            created_at=now,
            updated_at=now,
        )
    )

    summary = adapter.get_result_summary("job_summary")
    assert summary is not None
    assert summary.total_amount == Decimal("49.98")


def test_save_dead_letter_message_records_exhausted_failure() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)

    adapter.save_dead_letter_message(
        DeadLetterMessage(
            message_id="dlq_001",
            job_id="job_dlq",
            source_queue="local-processing",
            payload={"job_id": "job_dlq"},
            error_message="file not found",
            attempt_count=3,
            created_at=now,
        )
    )

    assert len(connection.dead_letter_messages) == 1

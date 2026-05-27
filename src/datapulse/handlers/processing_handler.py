"""Local processing handler for queued processing messages."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from uuid import uuid4

from datapulse.models import DeadLetterMessage
from datapulse.models import JobRecord
from datapulse.models import utc_now
from datapulse.pipeline.parser import parse_csv_orders
from datapulse.pipeline.transformer import transform_order_rows
from datapulse.pipeline.validator import validate_order_rows
from datapulse.pipeline.writer import write_processing_results
from datapulse.queue.base import ProcessingMessage
from datapulse.storage.base import StorageBackend


@dataclass(frozen=True)
class ProcessingResult:
    """Outcome returned by the local processing handler."""

    job_id: str
    status: str
    already_processed: bool = False


def handle_processing_message(
    message: ProcessingMessage,
    storage: StorageBackend,
    object_reader: Callable[[str, str], str],
    max_attempts: int = 3,
    clock: Callable[[], datetime] | None = None,
) -> ProcessingResult:
    """Process one queued message using local pipeline stages."""
    if clock is None:
        clock = utc_now

    job = storage.get_job(message.job_id)
    if job is None:
        raise KeyError(f"Job not found: {message.job_id}")

    if job.status == "SUCCEEDED":
        return ProcessingResult(job_id=message.job_id, status="SUCCEEDED", already_processed=True)

    now = clock()
    storage.update_job_status(message.job_id, "PROCESSING", metadata={"updated_at": now})

    try:
        csv_text = object_reader(message.bucket, message.object_key)
    except Exception as exc:  # noqa: BLE001 - handler records arbitrary local read failures.
        return _handle_processing_failure(message, storage, job, exc, max_attempts, now)

    raw_rows = parse_csv_orders(csv_text)
    validation_result = validate_order_rows(raw_rows, job_id=message.job_id)
    records = transform_order_rows(message.job_id, validation_result.valid_rows)
    summary = write_processing_results(
        storage=storage,
        job_id=message.job_id,
        total_records=len(raw_rows),
        records=records,
        errors=validation_result.errors,
    )

    status = "SUCCEEDED"
    last_error = None
    if summary.invalid_records > 0:
        status = "FAILED"
        last_error = f"{summary.invalid_records} row(s) failed validation"

    storage.update_job_status(
        message.job_id,
        status,
        metadata={
            "total_records": summary.total_records,
            "valid_records": summary.valid_records,
            "invalid_records": summary.invalid_records,
            "last_error": last_error,
            "updated_at": now,
        },
    )
    return ProcessingResult(job_id=message.job_id, status=status)


def _handle_processing_failure(
    message: ProcessingMessage,
    storage: StorageBackend,
    job: JobRecord,
    error: Exception,
    max_attempts: int,
    now: datetime,
) -> ProcessingResult:
    """Update retry metadata or save DLQ evidence for one processing failure."""
    attempt_count = job.attempt_count + 1
    error_message = str(error)

    if attempt_count >= max_attempts:
        storage.save_dead_letter_message(
            DeadLetterMessage(
                message_id=f"dlq_{uuid4().hex}",
                job_id=message.job_id,
                source_queue="local-processing",
                payload={
                    "job_id": message.job_id,
                    "bucket": message.bucket,
                    "object_key": message.object_key,
                    "object_key_hash": message.object_key_hash,
                },
                error_message=error_message,
                attempt_count=attempt_count,
                created_at=now,
            )
        )
        storage.update_job_status(
            message.job_id,
            "DEAD_LETTERED",
            metadata={
                "attempt_count": attempt_count,
                "last_error": error_message,
                "updated_at": now,
            },
        )
        return ProcessingResult(job_id=message.job_id, status="DEAD_LETTERED")

    storage.update_job_status(
        message.job_id,
        "FAILED",
        metadata={
            "attempt_count": attempt_count,
            "last_error": error_message,
            "next_attempt_at": now + timedelta(minutes=attempt_count),
            "updated_at": now,
        },
    )
    return ProcessingResult(job_id=message.job_id, status="FAILED")

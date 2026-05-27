"""MySQL implementation of the StorageBackend contract."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from decimal import Decimal
import json
from typing import Any
from typing import Mapping

from datapulse.models import DeadLetterMessage
from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.models import ProcessedRecord
from datapulse.models import ProcessingError
from datapulse.models import ResultSummary
from datapulse.models import validate_job_status
from datapulse.models import utc_now
from datapulse.storage.base import StorageBackend


class MySQLStorageAdapter(StorageBackend):
    """Store DataPulse job metadata in a MySQL-compatible database."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        """Create an adapter from a DB-API style connection factory."""
        self.connection_factory = connection_factory

    def create_job(self, job: JobRecord) -> JobRecord:
        """Insert a new job row and return the saved job."""
        validate_job_status(job.status)
        connection = self.connection_factory()

        # Insert only the M1 job metadata columns.
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    job_id,
                    status,
                    source_bucket,
                    source_key,
                    total_records,
                    valid_records,
                    invalid_records,
                    attempt_count,
                    last_error,
                    next_attempt_at,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job.job_id,
                    job.status,
                    job.source_bucket,
                    job.source_key,
                    job.total_records,
                    job.valid_records,
                    job.invalid_records,
                    job.attempt_count,
                    job.last_error,
                    job.next_attempt_at,
                    job.created_at,
                    job.updated_at,
                ),
            )

        connection.commit()
        return job

    def get_job(self, job_id: str) -> JobRecord | None:
        """Return one job row by primary key, or None when missing."""
        connection = self.connection_factory()

        # Query by primary key so status lookups stay simple and fast.
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                SELECT
                    job_id,
                    status,
                    source_bucket,
                    source_key,
                    total_records,
                    valid_records,
                    invalid_records,
                    attempt_count,
                    last_error,
                    next_attempt_at,
                    created_at,
                    updated_at
                FROM jobs
                WHERE job_id = %s
                """,
                (job_id,),
            )
            row = cursor.fetchone()

        if row is None:
            return None

        return self._job_from_row(row)

    def save_processed_records(self, job_id: str, records: list[ProcessedRecord]) -> None:
        """Insert processed records for one job, ignoring duplicate rows."""
        connection = self.connection_factory()
        with connection.cursor(dictionary=True) as cursor:
            for record in records:
                cursor.execute(
                    """
                    INSERT INTO processed_records (
                        record_id,
                        job_id,
                        row_number,
                        record_type,
                        amount,
                        currency,
                        payload_json,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        record_type = VALUES(record_type),
                        amount = VALUES(amount),
                        currency = VALUES(currency),
                        payload_json = VALUES(payload_json)
                    """,
                    (
                        record.record_id,
                        job_id,
                        record.row_number,
                        record.record_type,
                        record.amount,
                        record.currency,
                        json.dumps(record.payload),
                        record.created_at,
                    ),
                )

        connection.commit()

    def save_processing_error(self, error: ProcessingError) -> None:
        """Insert one processing error."""
        connection = self.connection_factory()
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                INSERT INTO processing_errors (
                    error_id,
                    job_id,
                    row_number,
                    error_code,
                    error_message,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    row_number = VALUES(row_number),
                    error_code = VALUES(error_code),
                    error_message = VALUES(error_message)
                """,
                (
                    error.error_id,
                    error.job_id,
                    error.row_number,
                    error.error_code,
                    error.error_message,
                    error.created_at,
                ),
            )

        connection.commit()

    def list_processing_errors(self, job_id: str) -> list[ProcessingError]:
        """Return processing errors for one job in created order."""
        connection = self.connection_factory()
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                SELECT
                    error_id,
                    job_id,
                    row_number,
                    error_code,
                    error_message,
                    created_at
                FROM processing_errors
                WHERE job_id = %s
                ORDER BY created_at
                """,
                (job_id,),
            )
            rows = cursor.fetchall()

        return [self._processing_error_from_row(row) for row in rows]

    def save_result_summary(self, summary: ResultSummary) -> None:
        """Insert or replace one result summary."""
        connection = self.connection_factory()
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                INSERT INTO result_summaries (
                    job_id,
                    total_records,
                    valid_records,
                    invalid_records,
                    total_amount,
                    summary_json,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    total_records = VALUES(total_records),
                    valid_records = VALUES(valid_records),
                    invalid_records = VALUES(invalid_records),
                    total_amount = VALUES(total_amount),
                    summary_json = VALUES(summary_json),
                    updated_at = VALUES(updated_at)
                """,
                (
                    summary.job_id,
                    summary.total_records,
                    summary.valid_records,
                    summary.invalid_records,
                    summary.total_amount,
                    json.dumps(summary.summary),
                    summary.created_at,
                    summary.updated_at,
                ),
            )

        connection.commit()

    def get_result_summary(self, job_id: str) -> ResultSummary | None:
        """Return one result summary by job id."""
        connection = self.connection_factory()
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                SELECT
                    job_id,
                    total_records,
                    valid_records,
                    invalid_records,
                    total_amount,
                    summary_json,
                    created_at,
                    updated_at
                FROM result_summaries
                WHERE job_id = %s
                """,
                (job_id,),
            )
            row = cursor.fetchone()

        if row is None:
            return None

        return self._result_summary_from_row(row)

    def save_dead_letter_message(self, message: DeadLetterMessage) -> None:
        """Insert one dead-letter evidence record."""
        connection = self.connection_factory()
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                INSERT INTO dead_letter_messages (
                    message_id,
                    job_id,
                    source_queue,
                    payload_json,
                    error_message,
                    attempt_count,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    message.message_id,
                    message.job_id,
                    message.source_queue,
                    json.dumps(message.payload),
                    message.error_message,
                    message.attempt_count,
                    message.created_at,
                ),
            )

        connection.commit()

    def record_file_manifest(self, manifest: FileManifest) -> FileManifest:
        """Insert metadata for one uploaded source file."""
        connection = self.connection_factory()

        # Store the idempotency key enforced by the MySQL unique constraint.
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                INSERT INTO file_manifests (
                    manifest_id,
                    job_id,
                    bucket,
                    object_key,
                    object_key_hash,
                    checksum,
                    content_type,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    manifest.manifest_id,
                    manifest.job_id,
                    manifest.bucket,
                    manifest.object_key,
                    manifest.object_key_hash,
                    manifest.checksum,
                    manifest.content_type,
                    manifest.created_at,
                ),
            )

        connection.commit()
        return manifest

    def find_job_by_file(self, bucket: str, object_key_hash: str) -> JobRecord | None:
        """Return the job created for one uploaded file."""
        connection = self.connection_factory()

        # Join manifest to jobs so idempotency returns the stored job metadata.
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                SELECT
                    jobs.job_id,
                    jobs.status,
                    jobs.source_bucket,
                    jobs.source_key,
                    jobs.total_records,
                    jobs.valid_records,
                    jobs.invalid_records,
                    jobs.attempt_count,
                    jobs.last_error,
                    jobs.next_attempt_at,
                    jobs.created_at,
                    jobs.updated_at
                FROM file_manifests
                INNER JOIN jobs ON jobs.job_id = file_manifests.job_id
                WHERE file_manifests.bucket = %s
                  AND file_manifests.object_key_hash = %s
                """,
                (bucket, object_key_hash),
            )
            row = cursor.fetchone()

        if row is None:
            return None

        return self._job_from_row(row)

    def update_job_status(
        self,
        job_id: str,
        status: str,
        metadata: Mapping[str, object] | None = None,
    ) -> JobRecord:
        """Update one job status and return the latest stored job."""
        validate_job_status(status)
        connection = self.connection_factory()
        update_metadata = {}
        if metadata is not None:
            update_metadata = dict(metadata)

        current_job = self.get_job(job_id)
        if current_job is None:
            raise KeyError(f"Job not found: {job_id}")

        last_error = update_metadata.get("last_error", current_job.last_error)
        updated_at = update_metadata.get("updated_at", utc_now())
        total_records = update_metadata.get("total_records", current_job.total_records)
        valid_records = update_metadata.get("valid_records", current_job.valid_records)
        invalid_records = update_metadata.get("invalid_records", current_job.invalid_records)
        attempt_count = update_metadata.get("attempt_count", current_job.attempt_count)
        next_attempt_at = update_metadata.get("next_attempt_at", current_job.next_attempt_at)

        # M1 keeps status updates narrow: lifecycle state plus error context.
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                UPDATE jobs
                SET status = %s,
                    last_error = %s,
                    total_records = %s,
                    valid_records = %s,
                    invalid_records = %s,
                    attempt_count = %s,
                    next_attempt_at = %s,
                    updated_at = %s
                WHERE job_id = %s
                """,
                (
                    status,
                    last_error,
                    total_records,
                    valid_records,
                    invalid_records,
                    attempt_count,
                    next_attempt_at,
                    updated_at,
                    job_id,
                ),
            )
            updated_count = cursor.rowcount

        if updated_count == 0:
            raise KeyError(f"Job not found: {job_id}")

        connection.commit()
        updated_job = self.get_job(job_id)
        if updated_job is None:
            raise KeyError(f"Job not found: {job_id}")

        return updated_job

    def _job_from_row(self, row: Mapping[str, Any]) -> JobRecord:
        """Convert a MySQL dictionary row into a JobRecord."""
        return JobRecord(
            job_id=str(row["job_id"]),
            status=str(row["status"]),
            source_bucket=self._optional_str(row.get("source_bucket")),
            source_key=self._optional_str(row.get("source_key")),
            total_records=int(row["total_records"]),
            valid_records=int(row["valid_records"]),
            invalid_records=int(row["invalid_records"]),
            attempt_count=int(row["attempt_count"]),
            last_error=self._optional_str(row.get("last_error")),
            next_attempt_at=self._optional_datetime(row.get("next_attempt_at")),
            created_at=self._required_datetime(row["created_at"]),
            updated_at=self._required_datetime(row["updated_at"]),
        )

    def _processing_error_from_row(self, row: Mapping[str, Any]) -> ProcessingError:
        """Convert a MySQL row into a ProcessingError."""
        row_number_value = row.get("row_number")
        row_number = None
        if row_number_value is not None:
            row_number = int(row_number_value)

        return ProcessingError(
            error_id=str(row["error_id"]),
            job_id=str(row["job_id"]),
            row_number=row_number,
            error_code=str(row["error_code"]),
            error_message=str(row["error_message"]),
            created_at=self._required_datetime(row["created_at"]),
        )

    def _result_summary_from_row(self, row: Mapping[str, Any]) -> ResultSummary:
        """Convert a MySQL row into a ResultSummary."""
        summary_json = row.get("summary_json")
        summary = {}
        if isinstance(summary_json, str):
            summary = json.loads(summary_json)
        elif isinstance(summary_json, dict):
            summary = summary_json

        total_amount = row.get("total_amount")
        if total_amount is not None and not isinstance(total_amount, Decimal):
            total_amount = Decimal(str(total_amount))

        return ResultSummary(
            job_id=str(row["job_id"]),
            total_records=int(row["total_records"]),
            valid_records=int(row["valid_records"]),
            invalid_records=int(row["invalid_records"]),
            total_amount=total_amount,
            summary=summary,
            created_at=self._required_datetime(row["created_at"]),
            updated_at=self._required_datetime(row["updated_at"]),
        )

    def _optional_str(self, value: object) -> str | None:
        """Return a string value or None for nullable columns."""
        if value is None:
            return None

        return str(value)

    def _optional_datetime(self, value: object) -> datetime | None:
        """Return a datetime value or None for nullable timestamp columns."""
        if value is None:
            return None

        return self._required_datetime(value)

    def _required_datetime(self, value: object) -> datetime:
        """Return a datetime value for non-null timestamp columns."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)

            return value

        raise TypeError(f"Expected datetime value, got {type(value)!r}")

"""MySQL implementation of the StorageBackend contract."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Mapping

from datapulse.models import JobRecord
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

        last_error = update_metadata.get("last_error")
        updated_at = update_metadata.get("updated_at", utc_now())

        # M1 keeps status updates narrow: lifecycle state plus error context.
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(
                """
                UPDATE jobs
                SET status = %s,
                    last_error = %s,
                    updated_at = %s
                WHERE job_id = %s
                """,
                (status, last_error, updated_at, job_id),
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

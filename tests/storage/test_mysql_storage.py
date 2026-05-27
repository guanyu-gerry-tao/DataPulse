from __future__ import annotations

from datetime import datetime
from datetime import timezone

import pytest

from datapulse.models import JobRecord
from datapulse.storage.mysql import MySQLStorageAdapter


class FakeMySQLConnection:
    """Small DB-API style connection used by storage unit tests."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, object]] = {}
        self.file_manifests: dict[tuple[str, str], dict[str, object]] = {}
        self.file_manifest_ids: set[str] = set()
        self.processed_records: dict[tuple[str, int], dict[str, object]] = {}
        self.processing_errors: list[dict[str, object]] = []
        self.result_summaries: dict[str, dict[str, object]] = {}
        self.dead_letter_messages: dict[str, dict[str, object]] = {}
        self.commit_count = 0

    def cursor(self, dictionary: bool = False) -> "FakeMySQLCursor":
        """Create a fake cursor that returns dictionary rows."""
        return FakeMySQLCursor(self, dictionary=dictionary)

    def commit(self) -> None:
        """Record that a transaction was committed."""
        self.commit_count = self.commit_count + 1


class FakeMySQLCursor:
    """Small cursor that supports the SQL statements used by the adapter."""

    def __init__(self, connection: FakeMySQLConnection, dictionary: bool) -> None:
        self.connection = connection
        self.dictionary = dictionary
        self.selected_row: dict[str, object] | None = None
        self.rowcount = -1

    def __enter__(self) -> "FakeMySQLCursor":
        """Return this cursor for context manager usage."""
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close the fake cursor context."""
        return None

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        """Execute the subset of SQL used by MySQLStorageAdapter."""
        normalized_sql = " ".join(sql.lower().split())

        # Store inserted job rows by primary key.
        if normalized_sql.startswith("insert into jobs"):
            assert len(params) == 12
            job_id = str(params[0])
            if job_id in self.connection.jobs:
                raise ValueError(f"Duplicate job_id: {job_id}")

            self.connection.jobs[job_id] = {
                "job_id": params[0],
                "status": params[1],
                "source_bucket": params[2],
                "source_key": params[3],
                "total_records": params[4],
                "valid_records": params[5],
                "invalid_records": params[6],
                "attempt_count": params[7],
                "last_error": params[8],
                "next_attempt_at": self._store_mysql_datetime(params[9]),
                "created_at": self._store_mysql_datetime(params[10]),
                "updated_at": self._store_mysql_datetime(params[11]),
            }
            self.rowcount = 1
            return

        # Return one job row for the requested primary key.
        if normalized_sql.startswith("select") and "from result_summaries" in normalized_sql:
            assert "where job_id = %s" in normalized_sql
            assert len(params) == 1
            job_id = str(params[0])
            self.selected_row = self.connection.result_summaries.get(job_id)
            if self.selected_row is None:
                self.rowcount = 0
            else:
                self.rowcount = 1
            return

        # Return processing errors for the requested job.
        if normalized_sql.startswith("select") and "from processing_errors" in normalized_sql:
            assert "where job_id = %s" in normalized_sql
            assert len(params) == 1
            job_id = str(params[0])
            self.selected_rows = [
                row for row in self.connection.processing_errors if row["job_id"] == job_id
            ]
            self.rowcount = len(self.selected_rows)
            return

        # Return one job row for the requested primary key.
        if (
            normalized_sql.startswith("select")
            and "from file_manifests" in normalized_sql
            and "inner join jobs" in normalized_sql
        ):
            assert "file_manifests.bucket = %s" in normalized_sql
            assert "file_manifests.object_key_hash = %s" in normalized_sql
            assert len(params) == 2
            bucket = str(params[0])
            object_key_hash = str(params[1])
            manifest = self.connection.file_manifests.get((bucket, object_key_hash))
            if manifest is None:
                self.selected_row = None
                self.rowcount = 0
                return

            job_id = str(manifest["job_id"])
            self.selected_row = self.connection.jobs.get(job_id)
            if self.selected_row is None:
                self.rowcount = 0
            else:
                self.rowcount = 1
            return

        # Return one job row for the requested primary key.
        if normalized_sql.startswith("select") and "from jobs" in normalized_sql:
            assert "where job_id = %s" in normalized_sql
            assert len(params) == 1
            job_id = str(params[0])
            self.selected_row = self.connection.jobs.get(job_id)
            if self.selected_row is None:
                self.rowcount = 0
            else:
                self.rowcount = 1
            return

        # Update status fields on an existing job row.
        if normalized_sql.startswith("update jobs"):
            assert "where job_id = %s" in normalized_sql
            assert len(params) == 9
            job_id = str(params[8])
            if job_id not in self.connection.jobs:
                self.rowcount = 0
                return

            row = self.connection.jobs[job_id]
            row["status"] = params[0]
            row["last_error"] = params[1]
            row["total_records"] = params[2]
            row["valid_records"] = params[3]
            row["invalid_records"] = params[4]
            row["attempt_count"] = params[5]
            row["next_attempt_at"] = self._store_mysql_datetime(params[6])
            row["updated_at"] = self._store_mysql_datetime(params[7])
            self.rowcount = 1
            return

        # Store one file manifest by bucket and object hash.
        if normalized_sql.startswith("insert into file_manifests"):
            assert len(params) == 8
            manifest_id = str(params[0])
            job_id = str(params[1])
            bucket = str(params[2])
            object_key_hash = str(params[4])
            manifest_key = (bucket, object_key_hash)
            if job_id not in self.connection.jobs:
                raise ValueError(f"Missing job_id for file manifest: {job_id}")

            if manifest_id in self.connection.file_manifest_ids:
                raise ValueError(f"Duplicate manifest_id: {manifest_id}")

            if manifest_key in self.connection.file_manifests:
                raise ValueError(f"Duplicate file manifest: {manifest_key}")

            self.connection.file_manifest_ids.add(manifest_id)
            self.connection.file_manifests[manifest_key] = {
                "manifest_id": manifest_id,
                "job_id": job_id,
                "bucket": params[2],
                "object_key": params[3],
                "object_key_hash": params[4],
                "checksum": params[5],
                "content_type": params[6],
                "created_at": self._store_mysql_datetime(params[7]),
            }
            self.rowcount = 1
            return

        # Store one processed record and ignore duplicate job-row writes.
        if normalized_sql.startswith("insert into processed_records"):
            assert len(params) == 8
            job_id = str(params[1])
            row_number = int(params[2])
            record_key = (job_id, row_number)
            if job_id not in self.connection.jobs:
                raise ValueError(f"Missing job_id for processed record: {job_id}")

            if record_key not in self.connection.processed_records:
                self.connection.processed_records[record_key] = {
                    "record_id": params[0],
                    "job_id": job_id,
                    "row_number": row_number,
                    "record_type": params[3],
                    "amount": params[4],
                    "currency": params[5],
                    "payload_json": params[6],
                    "created_at": self._store_mysql_datetime(params[7]),
                }
            self.rowcount = 1
            return

        # Store one processing error.
        if normalized_sql.startswith("insert into processing_errors"):
            assert len(params) == 6
            job_id = str(params[1])
            if job_id not in self.connection.jobs:
                raise ValueError(f"Missing job_id for processing error: {job_id}")

            self.connection.processing_errors.append(
                {
                    "error_id": params[0],
                    "job_id": job_id,
                    "row_number": params[2],
                    "error_code": params[3],
                    "error_message": params[4],
                    "created_at": self._store_mysql_datetime(params[5]),
                }
            )
            self.rowcount = 1
            return

        # Store one result summary by job id.
        if normalized_sql.startswith("insert into result_summaries"):
            assert len(params) == 8
            job_id = str(params[0])
            if job_id not in self.connection.jobs:
                raise ValueError(f"Missing job_id for result summary: {job_id}")

            self.connection.result_summaries[job_id] = {
                "job_id": job_id,
                "total_records": params[1],
                "valid_records": params[2],
                "invalid_records": params[3],
                "total_amount": params[4],
                "summary_json": params[5],
                "created_at": self._store_mysql_datetime(params[6]),
                "updated_at": self._store_mysql_datetime(params[7]),
            }
            self.rowcount = 1
            return

        # Store one dead-letter message.
        if normalized_sql.startswith("insert into dead_letter_messages"):
            assert len(params) == 7
            message_id = str(params[0])
            if message_id in self.connection.dead_letter_messages:
                raise ValueError(f"Duplicate message_id: {message_id}")

            self.connection.dead_letter_messages[message_id] = {
                "message_id": message_id,
                "job_id": params[1],
                "source_queue": params[2],
                "payload_json": params[3],
                "error_message": params[4],
                "attempt_count": params[5],
                "created_at": self._store_mysql_datetime(params[6]),
            }
            self.rowcount = 1
            return

        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        """Return the selected row from the most recent SELECT."""
        return self.selected_row

    def fetchall(self) -> list[dict[str, object]]:
        """Return selected rows from the most recent SELECT."""
        return getattr(self, "selected_rows", [])

    def _store_mysql_datetime(self, value: object) -> object:
        """Store datetimes the way a MySQL DATETIME column usually returns them."""
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.replace(tzinfo=None)

        return value


def test_create_job_then_get_job_returns_the_saved_job() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)

    created_job = adapter.create_job(
        JobRecord(
            job_id="job_001",
            status="QUEUED",
            source_bucket="local-bucket",
            source_key="uploads/orders.csv",
            created_at=now,
            updated_at=now,
        )
    )

    loaded_job = adapter.get_job("job_001")

    assert loaded_job == created_job
    assert loaded_job is not None
    assert loaded_job.job_id == "job_001"
    assert loaded_job.status == "QUEUED"
    assert connection.commit_count == 1


def test_get_missing_job_returns_none() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)

    loaded_job = adapter.get_job("missing_job")

    assert loaded_job is None


def test_update_job_status_returns_updated_job() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)
    later = datetime(2026, 5, 26, 12, 5, tzinfo=timezone.utc)
    adapter.create_job(
        JobRecord(
            job_id="job_002",
            status="QUEUED",
            source_bucket="local-bucket",
            source_key="uploads/orders.csv",
            created_at=now,
            updated_at=now,
        )
    )

    updated_job = adapter.update_job_status(
        "job_002",
        "PROCESSING",
        metadata={"updated_at": later},
    )

    assert updated_job.job_id == "job_002"
    assert updated_job.status == "PROCESSING"
    assert updated_job.updated_at == later
    assert connection.commit_count == 2


def test_update_job_status_rejects_invalid_status_before_writing() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    now = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)
    adapter.create_job(
        JobRecord(
            job_id="job_003",
            status="QUEUED",
            created_at=now,
            updated_at=now,
        )
    )

    with pytest.raises(ValueError, match="Invalid job status"):
        adapter.update_job_status("job_003", "NOT_A_STATUS")

    loaded_job = adapter.get_job("job_003")
    assert loaded_job is not None
    assert loaded_job.status == "QUEUED"
    assert connection.commit_count == 1


def test_update_missing_job_raises_key_error_without_commit() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)

    with pytest.raises(KeyError, match="Job not found"):
        adapter.update_job_status("missing_job", "PROCESSING")

    assert connection.commit_count == 0


def test_get_job_treats_mysql_naive_datetimes_as_utc() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)
    naive_created_at = datetime(2026, 5, 26, 12, 0)
    naive_next_attempt_at = datetime(2026, 5, 26, 12, 5)
    connection.jobs["job_naive"] = {
        "job_id": "job_naive",
        "status": "FAILED",
        "source_bucket": None,
        "source_key": None,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "attempt_count": 1,
        "last_error": "temporary failure",
        "next_attempt_at": naive_next_attempt_at,
        "created_at": naive_created_at,
        "updated_at": naive_created_at,
    }

    loaded_job = adapter.get_job("job_naive")

    assert loaded_job is not None
    assert loaded_job.created_at == naive_created_at.replace(tzinfo=timezone.utc)
    assert loaded_job.updated_at == naive_created_at.replace(tzinfo=timezone.utc)
    assert loaded_job.next_attempt_at == naive_next_attempt_at.replace(tzinfo=timezone.utc)


def test_job_record_rejects_invalid_status() -> None:
    connection = FakeMySQLConnection()

    with pytest.raises(ValueError, match="Invalid job status"):
        JobRecord(job_id="job_bad", status="NOT_A_STATUS")

    assert connection.jobs == {}
    assert connection.commit_count == 0

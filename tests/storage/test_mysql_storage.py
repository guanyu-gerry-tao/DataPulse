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
                "next_attempt_at": params[9],
                "created_at": params[10],
                "updated_at": params[11],
            }
            return

        # Return one job row for the requested primary key.
        if normalized_sql.startswith("select") and "from jobs" in normalized_sql:
            job_id = str(params[0])
            self.selected_row = self.connection.jobs.get(job_id)
            return

        # Update status fields on an existing job row.
        if normalized_sql.startswith("update jobs"):
            job_id = str(params[3])
            row = self.connection.jobs[job_id]
            row["status"] = params[0]
            row["last_error"] = params[1]
            row["updated_at"] = params[2]
            return

        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        """Return the selected row from the most recent SELECT."""
        return self.selected_row


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


def test_create_job_rejects_invalid_status() -> None:
    connection = FakeMySQLConnection()
    adapter = MySQLStorageAdapter(connection_factory=lambda: connection)

    with pytest.raises(ValueError, match="Invalid job status"):
        adapter.create_job(JobRecord(job_id="job_bad", status="NOT_A_STATUS"))

    assert connection.jobs == {}
    assert connection.commit_count == 0

from datapulse.db.mysql import apply_mysql_schema
from datapulse.db.mysql import load_mysql_schema_sql


class FakeSchemaConnection:
    """Small DB-API style connection used by schema helper tests."""

    def __init__(self) -> None:
        self.executed_statements: list[str] = []
        self.commit_count = 0

    def cursor(self) -> "FakeSchemaCursor":
        """Create a fake cursor for schema execution."""
        return FakeSchemaCursor(self)

    def commit(self) -> None:
        """Record that schema changes were committed."""
        self.commit_count = self.commit_count + 1


class FakeSchemaCursor:
    """Small cursor that records executed schema statements."""

    def __init__(self, connection: FakeSchemaConnection) -> None:
        self.connection = connection

    def __enter__(self) -> "FakeSchemaCursor":
        """Return this cursor for context manager usage."""
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close the fake cursor context."""
        return None

    def execute(self, statement: str) -> None:
        """Record one executed schema statement."""
        self.connection.executed_statements.append(statement)


def test_mysql_jobs_schema_is_repeatable_and_indexed() -> None:
    schema_sql = load_mysql_schema_sql()

    assert "CREATE TABLE IF NOT EXISTS jobs" in schema_sql
    assert "job_id VARCHAR(64) PRIMARY KEY" in schema_sql
    assert "status VARCHAR(32) NOT NULL" in schema_sql
    assert "INDEX idx_jobs_status_created_at (status, created_at)" in schema_sql
    assert "INDEX idx_jobs_created_at (created_at)" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS file_manifests" in schema_sql
    assert "UNIQUE KEY uq_file_manifest_bucket_key_hash" in schema_sql
    assert "CONSTRAINT fk_file_manifests_job" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS processed_records" in schema_sql
    assert "UNIQUE KEY uq_processed_records_job_row (job_id, row_number)" in schema_sql
    assert "CONSTRAINT fk_processed_records_job" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS processing_errors" in schema_sql
    assert "INDEX idx_processing_errors_job_created_at (job_id, created_at)" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS result_summaries" in schema_sql
    assert "CONSTRAINT fk_result_summaries_job" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS dead_letter_messages" in schema_sql
    assert "payload_json JSON NOT NULL" in schema_sql
    assert "INDEX idx_dead_letter_messages_created_at (created_at)" in schema_sql


def test_apply_mysql_schema_executes_jobs_schema_and_commits() -> None:
    connection = FakeSchemaConnection()

    apply_mysql_schema(connection)

    assert len(connection.executed_statements) == 6
    assert connection.executed_statements[0].startswith("CREATE TABLE IF NOT EXISTS jobs")
    assert connection.executed_statements[1].startswith(
        "CREATE TABLE IF NOT EXISTS file_manifests"
    )
    assert connection.executed_statements[2].startswith(
        "CREATE TABLE IF NOT EXISTS processed_records"
    )
    assert connection.executed_statements[3].startswith(
        "CREATE TABLE IF NOT EXISTS processing_errors"
    )
    assert connection.executed_statements[4].startswith(
        "CREATE TABLE IF NOT EXISTS result_summaries"
    )
    assert connection.executed_statements[5].startswith(
        "CREATE TABLE IF NOT EXISTS dead_letter_messages"
    )
    assert connection.commit_count == 1

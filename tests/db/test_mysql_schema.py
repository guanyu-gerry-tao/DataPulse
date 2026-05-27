from datapulse.db.mysql import load_mysql_schema_sql


def test_mysql_jobs_schema_is_repeatable_and_indexed() -> None:
    schema_sql = load_mysql_schema_sql()

    assert "CREATE TABLE IF NOT EXISTS jobs" in schema_sql
    assert "job_id VARCHAR(64) PRIMARY KEY" in schema_sql
    assert "status VARCHAR(32) NOT NULL" in schema_sql
    assert "INDEX idx_jobs_status_created_at (status, created_at)" in schema_sql
    assert "INDEX idx_jobs_created_at (created_at)" in schema_sql

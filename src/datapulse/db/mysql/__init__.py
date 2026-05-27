"""MySQL schema helpers."""

from __future__ import annotations

from importlib.resources import files
from typing import Any


def load_mysql_schema_sql() -> str:
    """Return the repeatable MySQL schema SQL for local initialization."""
    schema_path = files(__package__).joinpath("001_create_jobs.sql")
    return schema_path.read_text(encoding="utf-8")


def apply_mysql_schema(connection: Any) -> None:
    """Apply the M1 MySQL schema to a DB-API style connection."""
    schema_sql = load_mysql_schema_sql()

    # Split only on semicolons because the M1 migration contains plain DDL.
    statements = [statement.strip() for statement in schema_sql.split(";")]
    with connection.cursor() as cursor:
        for statement in statements:
            if statement:
                cursor.execute(statement)

    connection.commit()

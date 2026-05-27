"""MySQL schema helpers."""

from __future__ import annotations

from importlib.resources import files
from typing import Any


def load_mysql_schema_sql() -> str:
    """Return all repeatable MySQL schema SQL for local initialization."""
    schema_files = sorted(files(__package__).iterdir())
    schema_sql_parts = []
    for schema_file in schema_files:
        if schema_file.name.endswith(".sql"):
            schema_sql_parts.append(schema_file.read_text(encoding="utf-8"))

    return "\n".join(schema_sql_parts)


def apply_mysql_schema(connection: Any) -> None:
    """Apply local MySQL schema migrations to a DB-API style connection."""
    schema_sql = load_mysql_schema_sql()

    # Split only on semicolons because local migrations contain plain DDL.
    statements = [statement.strip() for statement in schema_sql.split(";")]
    with connection.cursor() as cursor:
        for statement in statements:
            if statement:
                cursor.execute(statement)

    connection.commit()

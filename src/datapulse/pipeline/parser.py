"""Parser stage for local CSV order datasets."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO


REQUIRED_COLUMNS = frozenset({"order_id", "amount", "currency"})


@dataclass(frozen=True)
class RawOrderRow:
    """Raw CSV row with its source row number."""

    row_number: int
    values: dict[str, str]


def parse_csv_orders(csv_text: str) -> list[RawOrderRow]:
    """Parse CSV order text into raw row objects."""
    reader = csv.DictReader(StringIO(csv_text))
    fieldnames = set(reader.fieldnames or [])
    missing_columns = REQUIRED_COLUMNS - fieldnames
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required CSV columns: {missing_list}")

    rows = []
    for row_index, row in enumerate(reader, start=1):
        clean_row = {}
        for key, value in row.items():
            if key is not None:
                clean_row[key] = "" if value is None else value.strip()

        rows.append(RawOrderRow(row_number=row_index, values=clean_row))

    return rows

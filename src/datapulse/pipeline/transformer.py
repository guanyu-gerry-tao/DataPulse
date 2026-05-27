"""Transformer stage for validated order rows."""

from __future__ import annotations

from datapulse.models import ProcessedRecord
from datapulse.models import utc_now
from datapulse.pipeline.validator import ValidOrderRow


def transform_order_rows(job_id: str, rows: list[ValidOrderRow]) -> list[ProcessedRecord]:
    """Transform valid order rows into processed records."""
    records: list[ProcessedRecord] = []

    for row in rows:
        records.append(
            ProcessedRecord(
                record_id=f"{job_id}:row:{row.row_number}",
                job_id=job_id,
                row_number=row.row_number,
                record_type="order",
                amount=row.amount,
                currency=row.currency,
                payload={"order_id": row.order_id},
                created_at=utc_now(),
            )
        )

    return records

"""Writer stage for processed order results."""

from __future__ import annotations

from decimal import Decimal

from datapulse.models import ProcessingError
from datapulse.models import ProcessedRecord
from datapulse.models import ResultSummary
from datapulse.models import utc_now
from datapulse.storage.base import StorageBackend


def write_processing_results(
    storage: StorageBackend,
    job_id: str,
    total_records: int,
    records: list[ProcessedRecord],
    errors: list[ProcessingError],
) -> ResultSummary:
    """Write processed records, row errors, and summary through storage."""
    storage.save_processed_records(job_id, records)
    for error in errors:
        storage.save_processing_error(error)

    total_amount = Decimal("0")
    for record in records:
        if record.amount is not None:
            total_amount = total_amount + record.amount

    now = utc_now()
    summary = ResultSummary(
        job_id=job_id,
        total_records=total_records,
        valid_records=len(records),
        invalid_records=len(errors),
        total_amount=total_amount,
        summary={"record_type": "order"},
        created_at=now,
        updated_at=now,
    )
    storage.save_result_summary(summary)
    return summary

"""Validation stage for local order rows."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from decimal import InvalidOperation

from datapulse.models import ProcessingError
from datapulse.models import utc_now
from datapulse.pipeline.parser import RawOrderRow


@dataclass(frozen=True)
class ValidOrderRow:
    """Validated order row ready for transformation."""

    row_number: int
    order_id: str
    amount: Decimal
    currency: str


@dataclass(frozen=True)
class ValidationResult:
    """Valid rows and explainable errors from validation."""

    valid_rows: list[ValidOrderRow]
    errors: list[ProcessingError]


def validate_order_rows(rows: list[RawOrderRow], job_id: str = "validation") -> ValidationResult:
    """Validate raw order rows and return valid rows plus row-level errors."""
    valid_rows: list[ValidOrderRow] = []
    errors: list[ProcessingError] = []

    for row in rows:
        order_id = row.values.get("order_id", "")
        amount_text = row.values.get("amount", "")
        currency = row.values.get("currency", "")

        if not order_id:
            errors.append(
                _row_error(job_id, row.row_number, "MISSING_ORDER_ID", "order_id is required")
            )
            continue

        try:
            amount = Decimal(amount_text)
        except InvalidOperation:
            errors.append(
                _row_error(job_id, row.row_number, "INVALID_AMOUNT", "amount must be numeric")
            )
            continue

        if not currency:
            errors.append(_row_error(job_id, row.row_number, "MISSING_CURRENCY", "currency is required"))
            continue

        valid_rows.append(
            ValidOrderRow(
                row_number=row.row_number,
                order_id=order_id,
                amount=amount,
                currency=currency,
            )
        )

    return ValidationResult(valid_rows=valid_rows, errors=errors)


def _row_error(job_id: str, row_number: int, error_code: str, error_message: str) -> ProcessingError:
    """Build a stable validation error for one row."""
    return ProcessingError(
        error_id=f"{job_id}:row:{row_number}:error:{error_code}",
        job_id=job_id,
        row_number=row_number,
        error_code=error_code,
        error_message=error_message,
        created_at=utc_now(),
    )

from decimal import Decimal

import pytest

from datapulse.pipeline.parser import parse_csv_orders
from datapulse.pipeline.transformer import transform_order_rows
from datapulse.pipeline.validator import validate_order_rows


def test_parser_validator_transformer_return_valid_records_and_errors() -> None:
    rows = parse_csv_orders("order_id,amount,currency\norder-001,19.99,USD\norder-002,bad,USD\n")

    validation_result = validate_order_rows(rows)
    transformed_records = transform_order_rows("job_001", validation_result.valid_rows)

    assert len(validation_result.valid_rows) == 1
    assert len(validation_result.errors) == 1
    assert validation_result.errors[0].row_number == 2
    assert validation_result.errors[0].error_code == "INVALID_AMOUNT"
    assert transformed_records[0].record_id == "job_001:row:1"
    assert transformed_records[0].amount == Decimal("19.99")


def test_parser_rejects_missing_required_columns() -> None:
    with pytest.raises(ValueError, match="Missing required CSV columns"):
        parse_csv_orders("order_id,amount\norder-001,19.99\n")

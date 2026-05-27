from __future__ import annotations

from decimal import Decimal

from benchmarks.query_benchmark import build_sample_records
from benchmarks.query_benchmark import compare_query_paths


def test_query_benchmark_verifies_raw_and_summary_metrics_are_equivalent() -> None:
    records = build_sample_records(record_count=50)

    result = compare_query_paths(records)

    assert result.record_count == 50
    assert result.raw_metrics == result.summary_metrics
    assert result.metrics_equivalent is True
    assert result.summary_metrics["total_amount"] == Decimal("1275.00")


def test_query_benchmark_rejects_empty_dataset() -> None:
    result = compare_query_paths([])

    assert result.record_count == 0
    assert result.raw_metrics == result.summary_metrics
    assert result.metrics_equivalent is True
    assert result.summary_metrics["total_amount"] == Decimal("0")

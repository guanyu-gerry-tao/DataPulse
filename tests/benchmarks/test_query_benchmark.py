from __future__ import annotations

from decimal import Decimal

from benchmarks.query_benchmark import build_sample_records
from benchmarks.query_benchmark import compare_query_paths
from datapulse.models import ResultSummary


def test_query_benchmark_verifies_raw_and_summary_metrics_are_equivalent() -> None:
    records = build_sample_records(record_count=50)

    result = compare_query_paths(records)

    expected_metrics = {
        "total_records": 50,
        "valid_records": 50,
        "invalid_records": 0,
        "total_amount": Decimal("1275"),
    }
    assert result.record_count == 50
    assert result.raw_metrics == expected_metrics
    assert result.raw_metrics == result.summary_metrics
    assert result.metrics_equivalent is True
    assert result.summary_metrics["total_amount"] == Decimal("1275")


def test_query_benchmark_accepts_empty_dataset_as_zero_metrics() -> None:
    result = compare_query_paths([])

    assert result.record_count == 0
    assert result.raw_metrics == result.summary_metrics
    assert result.metrics_equivalent is True
    assert result.summary_metrics["total_amount"] == Decimal("0")


def test_query_benchmark_detects_non_equivalent_summary_metrics() -> None:
    records = build_sample_records(record_count=3)
    mismatched_summary = ResultSummary(
        job_id="job_benchmark",
        total_records=3,
        valid_records=3,
        invalid_records=0,
        total_amount=Decimal("999"),
        summary={"record_type": "order"},
    )

    result = compare_query_paths(records, summary=mismatched_summary)

    assert result.raw_metrics["total_amount"] == Decimal("6")
    assert result.summary_metrics["total_amount"] == Decimal("999")
    assert result.metrics_equivalent is False

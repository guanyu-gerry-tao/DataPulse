"""Local query benchmark for raw aggregation versus summary lookup."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from decimal import Decimal
import json
from pathlib import Path
from time import perf_counter
from typing import Any

from datapulse.models import ProcessedRecord
from datapulse.models import ResultSummary


@dataclass(frozen=True)
class QueryBenchmarkResult:
    """Result from comparing raw aggregation and summary lookup paths."""

    record_count: int
    raw_metrics: dict[str, Decimal | int]
    summary_metrics: dict[str, Decimal | int]
    metrics_equivalent: bool
    raw_query_seconds: float
    summary_query_seconds: float


def build_sample_records(record_count: int) -> list[ProcessedRecord]:
    """Build deterministic sample records for benchmark and tests."""
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    records = []
    for index in range(1, record_count + 1):
        amount = Decimal(index)
        records.append(
            ProcessedRecord(
                record_id=f"benchmark:row:{index}",
                job_id="job_benchmark",
                row_number=index,
                record_type="order",
                amount=amount,
                currency="USD",
                payload={"order_id": f"order-{index:06d}"},
                created_at=now,
            )
        )

    return records


def compare_query_paths(records: list[ProcessedRecord]) -> QueryBenchmarkResult:
    """Compare raw aggregation with precomputed summary lookup."""
    raw_start = perf_counter()
    raw_metrics = aggregate_raw_records(records)
    raw_query_seconds = perf_counter() - raw_start

    summary = build_result_summary("job_benchmark", records)
    summary_start = perf_counter()
    summary_metrics = summary_to_metrics(summary)
    summary_query_seconds = perf_counter() - summary_start

    return QueryBenchmarkResult(
        record_count=len(records),
        raw_metrics=raw_metrics,
        summary_metrics=summary_metrics,
        metrics_equivalent=raw_metrics == summary_metrics,
        raw_query_seconds=raw_query_seconds,
        summary_query_seconds=summary_query_seconds,
    )


def aggregate_raw_records(records: list[ProcessedRecord]) -> dict[str, Decimal | int]:
    """Aggregate metrics by scanning processed records."""
    total_amount = Decimal("0")
    for record in records:
        if record.amount is not None:
            total_amount = total_amount + record.amount

    return {
        "total_records": len(records),
        "valid_records": len(records),
        "invalid_records": 0,
        "total_amount": total_amount,
    }


def build_result_summary(job_id: str, records: list[ProcessedRecord]) -> ResultSummary:
    """Build the summary/read-model representation for one job."""
    metrics = aggregate_raw_records(records)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    return ResultSummary(
        job_id=job_id,
        total_records=int(metrics["total_records"]),
        valid_records=int(metrics["valid_records"]),
        invalid_records=int(metrics["invalid_records"]),
        total_amount=metrics["total_amount"],
        summary={"record_type": "order"},
        created_at=now,
        updated_at=now,
    )


def summary_to_metrics(summary: ResultSummary) -> dict[str, Decimal | int]:
    """Convert a summary/read-model row into benchmark metrics."""
    total_amount = Decimal("0")
    if summary.total_amount is not None:
        total_amount = Decimal(str(summary.total_amount))

    return {
        "total_records": summary.total_records,
        "valid_records": summary.valid_records,
        "invalid_records": summary.invalid_records,
        "total_amount": total_amount,
    }


def write_result(result: QueryBenchmarkResult, output_path: Path) -> None:
    """Write a benchmark result JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result_to_json(result), indent=2) + "\n")


def result_to_json(result: QueryBenchmarkResult) -> dict[str, Any]:
    """Convert a benchmark result into JSON-compatible values."""
    return {
        "record_count": result.record_count,
        "raw_metrics": _metrics_to_json(result.raw_metrics),
        "summary_metrics": _metrics_to_json(result.summary_metrics),
        "metrics_equivalent": result.metrics_equivalent,
        "raw_query_seconds": result.raw_query_seconds,
        "summary_query_seconds": result.summary_query_seconds,
    }


def _metrics_to_json(metrics: dict[str, Decimal | int]) -> dict[str, int | str]:
    """Serialize benchmark metrics for JSON output."""
    serialized: dict[str, int | str] = {}
    for key, value in metrics.items():
        if isinstance(value, Decimal):
            serialized[key] = str(value)
        else:
            serialized[key] = value

    return serialized


def main() -> None:
    """Run the local benchmark and write ignored result evidence."""
    records = build_sample_records(record_count=1000)
    result = compare_query_paths(records)
    write_result(result, Path("benchmarks/results/query_benchmark.json"))
    print(json.dumps(result_to_json(result), indent=2))


if __name__ == "__main__":
    main()

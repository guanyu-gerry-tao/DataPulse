"""Minimal job handler helpers for M1 local workflows."""

from __future__ import annotations

from typing import Any

from datapulse.storage.base import StorageBackend


def get_job_status_response(storage: StorageBackend, job_id: str) -> dict[str, Any]:
    """Build a small response dictionary for a job status lookup."""
    job = storage.get_job(job_id)
    if job is None:
        return {"statusCode": 404, "body": {"message": "Job not found"}}

    # Keep the handler independent from the concrete storage adapter.
    return {
        "statusCode": 200,
        "body": {
            "job_id": job.job_id,
            "status": job.status,
            "total_records": job.total_records,
            "valid_records": job.valid_records,
            "invalid_records": job.invalid_records,
        },
    }

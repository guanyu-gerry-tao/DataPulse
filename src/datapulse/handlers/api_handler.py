"""Local REST-style API handler for job submission and lookup."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from hashlib import sha256
import json
from pathlib import PurePosixPath
from typing import Any
from uuid import uuid4

from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.models import ResultSummary
from datapulse.models import utc_now
from datapulse.queue.base import ProcessingMessage
from datapulse.queue.base import QueueAdapter
from datapulse.storage.base import StorageBackend


SUPPORTED_SUBMIT_EXTENSIONS = frozenset({".csv", ".json"})


def handle_api_request(
    event: dict[str, Any],
    storage: StorageBackend,
    queue: QueueAdapter,
    job_id_factory: Callable[[], str] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Route a local API Gateway-style event to a DataPulse API response."""
    method = _request_method(event)
    path = _request_path(event)

    if method == "POST" and path == "/jobs":
        return submit_job_response(
            event,
            storage=storage,
            queue=queue,
            job_id_factory=job_id_factory,
            clock=clock,
        )

    if method == "GET" and path.endswith("/result"):
        job_id = _path_job_id(event)
        return get_result_summary_response(storage, job_id)

    if method == "GET":
        job_id = _path_job_id(event)
        return get_job_status_response(storage, job_id)

    return _json_response(404, {"message": "Route not found"})


def submit_job_response(
    event: dict[str, Any],
    storage: StorageBackend,
    queue: QueueAdapter,
    job_id_factory: Callable[[], str] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Create or reuse a queued processing job from an API request."""
    body = _json_body(event)
    bucket = _required_body_string(body, "bucket")
    object_key = _required_body_string(body, "object_key")
    checksum = _optional_body_string(body, "checksum")
    content_type = _optional_body_string(body, "content_type")
    _validate_supported_file(object_key)

    object_key_hash = sha256(object_key.encode("utf-8")).hexdigest()
    existing_job = storage.find_job_by_file(bucket, object_key_hash)
    if existing_job is not None:
        return _json_response(
            200,
            {
                "job_id": existing_job.job_id,
                "status": existing_job.status,
                "created": False,
            },
        )

    if job_id_factory is None:
        job_id_factory = _default_job_id

    if clock is None:
        clock = utc_now

    now = clock()
    job_id = job_id_factory()

    # Store job and manifest before publishing the processing message.
    job = storage.create_job(
        JobRecord(
            job_id=job_id,
            status="QUEUED",
            source_bucket=bucket,
            source_key=object_key,
            created_at=now,
            updated_at=now,
        )
    )
    storage.record_file_manifest(
        FileManifest(
            manifest_id=f"manifest_{job.job_id}",
            job_id=job.job_id,
            bucket=bucket,
            object_key=object_key,
            object_key_hash=object_key_hash,
            checksum=checksum,
            content_type=content_type,
            created_at=now,
        )
    )
    queue.enqueue(
        ProcessingMessage(
            job_id=job.job_id,
            bucket=bucket,
            object_key=object_key,
            object_key_hash=object_key_hash,
        )
    )

    return _json_response(
        202,
        {
            "job_id": job.job_id,
            "status": job.status,
            "created": True,
            "object_key_hash": object_key_hash,
        },
    )


def get_job_status_response(storage: StorageBackend, job_id: str) -> dict[str, Any]:
    """Return job status and counters for one job."""
    job = storage.get_job(job_id)
    if job is None:
        return _json_response(404, {"message": "Job not found"})

    return _json_response(
        200,
        {
            "job_id": job.job_id,
            "status": job.status,
            "source_bucket": job.source_bucket,
            "source_key": job.source_key,
            "total_records": job.total_records,
            "valid_records": job.valid_records,
            "invalid_records": job.invalid_records,
            "attempt_count": job.attempt_count,
            "last_error": job.last_error,
        },
    )


def get_result_summary_response(storage: StorageBackend, job_id: str) -> dict[str, Any]:
    """Return a processed job result summary."""
    summary = storage.get_result_summary(job_id)
    if summary is None:
        return _json_response(404, {"message": "Result summary not found"})

    return _json_response(200, _summary_body(summary))


def _summary_body(summary: ResultSummary) -> dict[str, Any]:
    """Serialize a ResultSummary into a JSON-compatible response body."""
    total_amount = None
    if summary.total_amount is not None:
        total_amount = str(summary.total_amount)

    return {
        "job_id": summary.job_id,
        "total_records": summary.total_records,
        "valid_records": summary.valid_records,
        "invalid_records": summary.invalid_records,
        "total_amount": total_amount,
        "summary": summary.summary,
    }


def _request_method(event: dict[str, Any]) -> str:
    """Extract the HTTP method from an API Gateway-style event."""
    request_context = event.get("requestContext", {})
    if isinstance(request_context, dict):
        http_context = request_context.get("http", {})
        if isinstance(http_context, dict):
            method = http_context.get("method")
            if isinstance(method, str):
                return method.upper()

    method = event.get("httpMethod")
    if isinstance(method, str):
        return method.upper()

    return ""


def _request_path(event: dict[str, Any]) -> str:
    """Extract the HTTP path from an API Gateway-style event."""
    request_context = event.get("requestContext", {})
    if isinstance(request_context, dict):
        http_context = request_context.get("http", {})
        if isinstance(http_context, dict):
            path = http_context.get("path")
            if isinstance(path, str):
                return path

    path = event.get("path")
    if isinstance(path, str):
        return path

    return ""


def _path_job_id(event: dict[str, Any]) -> str:
    """Extract job_id from path parameters."""
    path_parameters = event.get("pathParameters", {})
    if isinstance(path_parameters, dict):
        job_id = path_parameters.get("job_id")
        if isinstance(job_id, str) and job_id.strip():
            return job_id

    path_parts = [part for part in _request_path(event).split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] == "jobs":
        return path_parts[1]

    return ""


def _json_body(event: dict[str, Any]) -> dict[str, Any]:
    """Parse a JSON request body."""
    body = event.get("body", "{}")
    if body is None:
        return {}

    if isinstance(body, dict):
        return body

    if isinstance(body, str):
        parsed_body = json.loads(body)
        if isinstance(parsed_body, dict):
            return parsed_body

    raise ValueError("Request body must be a JSON object")


def _required_body_string(body: dict[str, Any], field_name: str) -> str:
    """Return a required non-empty string body field."""
    value = body.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")

    return value


def _optional_body_string(body: dict[str, Any], field_name: str) -> str | None:
    """Return an optional string body field."""
    value = body.get(field_name)
    if value is None:
        return None

    return str(value)


def _validate_supported_file(object_key: str) -> None:
    """Raise ValueError when the submitted file type is unsupported."""
    suffix = PurePosixPath(object_key).suffix.lower()
    if suffix not in SUPPORTED_SUBMIT_EXTENSIONS:
        raise ValueError(f"Unsupported file extension: {suffix}")


def _json_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON API response dictionary."""
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _default_job_id() -> str:
    """Create a unique local job id."""
    return f"job_{uuid4().hex}"

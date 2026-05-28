from __future__ import annotations

import json
from datetime import datetime
from datetime import timezone
from decimal import Decimal

from datapulse.handlers.api_handler import handle_api_request
from datapulse.models import JobRecord
from datapulse.models import ResultSummary
from datapulse.queue.memory import InMemoryQueueAdapter
from datapulse.storage.dynamodb import DynamoDBStorageAdapter
from datapulse.storage.dynamodb import InMemoryDynamoDBTable


def test_api_handler_submits_job_and_enqueues_processing_message() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    event = {
        "requestContext": {"http": {"method": "POST", "path": "/jobs"}},
        "body": json.dumps(
            {
                "bucket": "datapulse-local-raw",
                "object_key": "uploads/orders.csv",
                "checksum": "etag-001",
                "content_type": "text/csv",
            }
        ),
    }

    response = handle_api_request(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_api_001",
        clock=lambda: now,
    )

    body = json.loads(response["body"])
    loaded_job = storage.get_job("job_api_001")
    assert response["statusCode"] == 202
    assert body["job_id"] == "job_api_001"
    assert body["status"] == "QUEUED"
    assert loaded_job is not None
    assert loaded_job.source_bucket == "datapulse-local-raw"
    assert len(queue.messages) == 1
    assert queue.messages[0].job_id == "job_api_001"


def test_api_handler_returns_existing_job_for_duplicate_submit() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    event = {
        "requestContext": {"http": {"method": "POST", "path": "/jobs"}},
        "body": json.dumps(
            {
                "bucket": "datapulse-local-raw",
                "object_key": "uploads/orders.csv",
                "checksum": "etag-001",
                "content_type": "text/csv",
            }
        ),
    }

    first_response = handle_api_request(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_api_001",
        clock=lambda: now,
    )
    second_response = handle_api_request(
        event,
        storage=storage,
        queue=queue,
        job_id_factory=lambda: "job_api_002",
        clock=lambda: now,
    )

    first_body = json.loads(first_response["body"])
    second_body = json.loads(second_response["body"])
    assert first_response["statusCode"] == 202
    assert second_response["statusCode"] == 200
    assert first_body["job_id"] == "job_api_001"
    assert second_body["job_id"] == "job_api_001"
    assert second_body["created"] is False
    assert len(queue.messages) == 1


def test_api_handler_gets_job_status() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    storage.create_job(
        JobRecord(
            job_id="job_api_status",
            status="FAILED",
            total_records=3,
            valid_records=2,
            invalid_records=1,
            last_error="1 row(s) failed validation",
            created_at=now,
            updated_at=now,
        )
    )

    response = handle_api_request(
        {
            "requestContext": {"http": {"method": "GET", "path": "/jobs/job_api_status"}},
            "pathParameters": {"job_id": "job_api_status"},
        },
        storage=storage,
        queue=queue,
    )

    body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert body["job_id"] == "job_api_status"
    assert body["status"] == "FAILED"
    assert body["invalid_records"] == 1
    assert body["last_error"] == "1 row(s) failed validation"


def test_api_handler_gets_result_summary() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    storage.create_job(
        JobRecord(
            job_id="job_api_result",
            status="SUCCEEDED",
            created_at=now,
            updated_at=now,
        )
    )
    storage.save_result_summary(
        ResultSummary(
            job_id="job_api_result",
            total_records=2,
            valid_records=2,
            invalid_records=0,
            total_amount=Decimal("49.98"),
            summary={"record_type": "order"},
            created_at=now,
            updated_at=now,
        )
    )

    response = handle_api_request(
        {
            "requestContext": {
                "http": {"method": "GET", "path": "/jobs/job_api_result/result"}
            },
            "pathParameters": {"job_id": "job_api_result"},
        },
        storage=storage,
        queue=queue,
    )

    body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert body["job_id"] == "job_api_result"
    assert body["total_records"] == 2
    assert body["total_amount"] == "49.98"


def test_api_handler_returns_not_found_for_missing_job() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()

    response = handle_api_request(
        {
            "requestContext": {"http": {"method": "GET", "path": "/jobs/missing_job"}},
            "pathParameters": {"job_id": "missing_job"},
        },
        storage=storage,
        queue=queue,
    )

    body = json.loads(response["body"])
    assert response["statusCode"] == 404
    assert body["message"] == "Job not found"


def test_api_handler_returns_bad_request_for_invalid_json_body() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()

    response = handle_api_request(
        {
            "requestContext": {"http": {"method": "POST", "path": "/jobs"}},
            "body": "{not-json",
        },
        storage=storage,
        queue=queue,
    )

    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["message"] == "Request body must be valid JSON"
    assert len(queue.messages) == 0


def test_api_handler_returns_bad_request_for_missing_submit_field() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()

    response = handle_api_request(
        {
            "requestContext": {"http": {"method": "POST", "path": "/jobs"}},
            "body": json.dumps({"object_key": "uploads/orders.csv"}),
        },
        storage=storage,
        queue=queue,
    )

    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["message"] == "bucket is required"
    assert len(queue.messages) == 0


def test_api_handler_returns_bad_request_for_unsupported_submit_extension() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()

    response = handle_api_request(
        {
            "requestContext": {"http": {"method": "POST", "path": "/jobs"}},
            "body": json.dumps(
                {
                    "bucket": "datapulse-local-raw",
                    "object_key": "uploads/readme.txt",
                }
            ),
        },
        storage=storage,
        queue=queue,
    )

    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["message"] == "Unsupported file extension: .txt"
    assert len(queue.messages) == 0


def test_api_handler_returns_route_not_found_for_unknown_get_path() -> None:
    storage = DynamoDBStorageAdapter(table=InMemoryDynamoDBTable())
    queue = InMemoryQueueAdapter()

    response = handle_api_request(
        {"requestContext": {"http": {"method": "GET", "path": "/health"}}},
        storage=storage,
        queue=queue,
    )

    body = json.loads(response["body"])
    assert response["statusCode"] == 404
    assert body["message"] == "Route not found"

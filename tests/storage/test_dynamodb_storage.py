from __future__ import annotations

from datetime import datetime
from datetime import timezone

from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.storage.dynamodb import DynamoDBStorageAdapter
from datapulse.storage.dynamodb import InMemoryDynamoDBTable


def test_dynamodb_adapter_writes_job_metadata_with_single_table_keys() -> None:
    table = InMemoryDynamoDBTable()
    adapter = DynamoDBStorageAdapter(table=table)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)

    adapter.create_job(
        JobRecord(
            job_id="job_dynamodb_keys",
            status="QUEUED",
            source_bucket="datapulse-local-raw",
            source_key="uploads/orders.csv",
            created_at=now,
            updated_at=now,
        )
    )

    item = table.get_item("JOB#job_dynamodb_keys", "METADATA")
    assert item is not None
    assert item["entity_type"] == "JOB"
    assert item["job_id"] == "job_dynamodb_keys"
    assert item["status"] == "QUEUED"


def test_dynamodb_adapter_writes_manifest_with_file_lookup_gsi() -> None:
    table = InMemoryDynamoDBTable()
    adapter = DynamoDBStorageAdapter(table=table)
    now = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
    adapter.create_job(
        JobRecord(
            job_id="job_dynamodb_manifest",
            status="QUEUED",
            created_at=now,
            updated_at=now,
        )
    )

    adapter.record_file_manifest(
        FileManifest(
            manifest_id="manifest_dynamodb",
            job_id="job_dynamodb_manifest",
            bucket="datapulse-local-raw",
            object_key="uploads/orders.csv",
            object_key_hash="hash-dynamodb",
            created_at=now,
        )
    )

    rows = table.query_gsi1("FILE#datapulse-local-raw#hash-dynamodb")
    assert len(rows) == 1
    assert rows[0]["entity_type"] == "MANIFEST"
    assert rows[0]["job_id"] == "job_dynamodb_manifest"

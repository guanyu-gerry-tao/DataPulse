"""DynamoDB-style storage adapter for local contract testing."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from typing import Any
from typing import Mapping

from datapulse.models import DeadLetterMessage
from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.models import ProcessedRecord
from datapulse.models import ProcessingError
from datapulse.models import ResultSummary
from datapulse.models import validate_job_status
from datapulse.models import utc_now
from datapulse.storage.base import StorageBackend


class InMemoryDynamoDBTable:
    """Small DynamoDB-like table with PK/SK items and a file lookup GSI."""

    def __init__(self) -> None:
        """Create an empty local DynamoDB-style table."""
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.gsi1_items: dict[tuple[str, str], tuple[str, str]] = {}

    def put_item(self, item: Mapping[str, Any]) -> None:
        """Store one item and update local GSI entries."""
        stored_item = deepcopy(dict(item))
        key = (str(stored_item["PK"]), str(stored_item["SK"]))
        self.items[key] = stored_item

        # GSI1 supports file idempotency lookup by bucket and object hash.
        gsi1_pk = stored_item.get("GSI1PK")
        gsi1_sk = stored_item.get("GSI1SK")
        if gsi1_pk is not None and gsi1_sk is not None:
            self.gsi1_items[(str(gsi1_pk), str(gsi1_sk))] = key

    def get_item(self, pk: str, sk: str) -> dict[str, Any] | None:
        """Return one item by primary key."""
        item = self.items.get((pk, sk))
        if item is None:
            return None

        return deepcopy(item)

    def query_partition(self, pk: str, sk_prefix: str) -> list[dict[str, Any]]:
        """Return items for one partition whose sort key matches a prefix."""
        rows = []
        for (item_pk, item_sk), item in self.items.items():
            if item_pk == pk and item_sk.startswith(sk_prefix):
                rows.append(deepcopy(item))

        return rows

    def query_gsi1(self, gsi1_pk: str) -> list[dict[str, Any]]:
        """Return items from the local file-idempotency GSI."""
        rows = []
        for (stored_gsi1_pk, _stored_gsi1_sk), item_key in self.gsi1_items.items():
            if stored_gsi1_pk == gsi1_pk:
                rows.append(deepcopy(self.items[item_key]))

        return rows


class DynamoDBStorageAdapter(StorageBackend):
    """Store DataPulse entities in a DynamoDB single-table shape."""

    def __init__(self, table: InMemoryDynamoDBTable) -> None:
        """Create an adapter backed by a DynamoDB-like table object."""
        self.table = table

    def create_job(self, job: JobRecord) -> JobRecord:
        """Persist a new job metadata item."""
        validate_job_status(job.status)
        existing_job = self.get_job(job.job_id)
        if existing_job is not None:
            raise ValueError(f"Duplicate job_id: {job.job_id}")

        self.table.put_item(self._job_to_item(job))
        return job

    def get_job(self, job_id: str) -> JobRecord | None:
        """Return one job by id, or None when it does not exist."""
        item = self.table.get_item(self._job_pk(job_id), "METADATA")
        if item is None:
            return None

        return self._job_from_item(item)

    def update_job_status(
        self,
        job_id: str,
        status: str,
        metadata: Mapping[str, object] | None = None,
    ) -> JobRecord:
        """Update one job metadata item and preserve unspecified fields."""
        validate_job_status(status)
        existing_job = self.get_job(job_id)
        if existing_job is None:
            raise KeyError(f"Job not found: {job_id}")

        update_metadata = {}
        if metadata is not None:
            update_metadata = dict(metadata)

        updated_job = JobRecord(
            job_id=existing_job.job_id,
            status=status,
            source_bucket=existing_job.source_bucket,
            source_key=existing_job.source_key,
            total_records=int(update_metadata.get("total_records", existing_job.total_records)),
            valid_records=int(update_metadata.get("valid_records", existing_job.valid_records)),
            invalid_records=int(update_metadata.get("invalid_records", existing_job.invalid_records)),
            attempt_count=int(update_metadata.get("attempt_count", existing_job.attempt_count)),
            last_error=self._optional_str(
                update_metadata.get("last_error", existing_job.last_error)
            ),
            next_attempt_at=self._optional_datetime(
                update_metadata.get("next_attempt_at", existing_job.next_attempt_at)
            ),
            created_at=existing_job.created_at,
            updated_at=self._required_datetime(update_metadata.get("updated_at", utc_now())),
        )
        self.table.put_item(self._job_to_item(updated_job))
        return updated_job

    def record_file_manifest(self, manifest: FileManifest) -> FileManifest:
        """Persist file metadata with a DynamoDB GSI idempotency key."""
        if self.get_job(manifest.job_id) is None:
            raise ValueError(f"Missing job_id for file manifest: {manifest.job_id}")

        self.table.put_item(
            {
                "PK": self._job_pk(manifest.job_id),
                "SK": f"MANIFEST#{manifest.object_key_hash}",
                "entity_type": "MANIFEST",
                "manifest_id": manifest.manifest_id,
                "job_id": manifest.job_id,
                "bucket": manifest.bucket,
                "object_key": manifest.object_key,
                "object_key_hash": manifest.object_key_hash,
                "checksum": manifest.checksum,
                "content_type": manifest.content_type,
                "created_at": self._datetime_to_string(manifest.created_at),
                "GSI1PK": self._file_gsi_pk(manifest.bucket, manifest.object_key_hash),
                "GSI1SK": self._job_pk(manifest.job_id),
            }
        )
        return manifest

    def find_job_by_file(self, bucket: str, object_key_hash: str) -> JobRecord | None:
        """Return the job associated with one uploaded file."""
        rows = self.table.query_gsi1(self._file_gsi_pk(bucket, object_key_hash))
        if not rows:
            return None

        job_id = str(rows[0]["job_id"])
        return self.get_job(job_id)

    def save_processed_records(self, job_id: str, records: list[ProcessedRecord]) -> None:
        """Persist processed records using a stable job-row sort key."""
        if self.get_job(job_id) is None:
            raise ValueError(f"Missing job_id for processed record: {job_id}")

        for record in records:
            self.table.put_item(
                {
                    "PK": self._job_pk(job_id),
                    "SK": f"RECORD#{record.row_number:08d}",
                    "entity_type": "RECORD",
                    "record_id": record.record_id,
                    "job_id": job_id,
                    "row_number": record.row_number,
                    "record_type": record.record_type,
                    "amount": self._decimal_to_string(record.amount),
                    "currency": record.currency,
                    "payload": deepcopy(record.payload),
                    "created_at": self._datetime_to_string(record.created_at),
                }
            )

    def save_processing_error(self, error: ProcessingError) -> None:
        """Persist one processing error by stable error id."""
        if self.get_job(error.job_id) is None:
            raise ValueError(f"Missing job_id for processing error: {error.job_id}")

        self.table.put_item(
            {
                "PK": self._job_pk(error.job_id),
                "SK": f"ERROR#{error.error_id}",
                "entity_type": "ERROR",
                "error_id": error.error_id,
                "job_id": error.job_id,
                "row_number": error.row_number,
                "error_code": error.error_code,
                "error_message": error.error_message,
                "created_at": self._datetime_to_string(error.created_at),
            }
        )

    def list_processing_errors(self, job_id: str) -> list[ProcessingError]:
        """Return processing errors for one job in creation order."""
        items = self.table.query_partition(self._job_pk(job_id), "ERROR#")
        errors = [self._processing_error_from_item(item) for item in items]
        return sorted(errors, key=lambda error: (error.created_at, error.error_id))

    def save_result_summary(self, summary: ResultSummary) -> None:
        """Persist or replace the result summary for one job."""
        if self.get_job(summary.job_id) is None:
            raise ValueError(f"Missing job_id for result summary: {summary.job_id}")

        created_at = summary.created_at
        existing_summary = self.get_result_summary(summary.job_id)
        if existing_summary is not None:
            created_at = existing_summary.created_at

        self.table.put_item(
            {
                "PK": self._job_pk(summary.job_id),
                "SK": "RESULT_SUMMARY",
                "entity_type": "SUMMARY",
                "job_id": summary.job_id,
                "total_records": summary.total_records,
                "valid_records": summary.valid_records,
                "invalid_records": summary.invalid_records,
                "total_amount": self._decimal_to_string(summary.total_amount),
                "summary": deepcopy(summary.summary),
                "created_at": self._datetime_to_string(created_at),
                "updated_at": self._datetime_to_string(summary.updated_at),
            }
        )

    def get_result_summary(self, job_id: str) -> ResultSummary | None:
        """Return the result summary for one job."""
        item = self.table.get_item(self._job_pk(job_id), "RESULT_SUMMARY")
        if item is None:
            return None

        return self._result_summary_from_item(item)

    def save_dead_letter_message(self, message: DeadLetterMessage) -> None:
        """Persist dead-letter evidence for an exhausted message."""
        job_partition = "JOB#UNKNOWN"
        if message.job_id is not None:
            job_partition = self._job_pk(message.job_id)

        self.table.put_item(
            {
                "PK": job_partition,
                "SK": f"DLQ#{message.message_id}",
                "entity_type": "DLQ",
                "message_id": message.message_id,
                "job_id": message.job_id,
                "source_queue": message.source_queue,
                "payload": deepcopy(message.payload),
                "error_message": message.error_message,
                "attempt_count": message.attempt_count,
                "created_at": self._datetime_to_string(message.created_at),
            }
        )

    def _job_to_item(self, job: JobRecord) -> dict[str, Any]:
        """Convert a JobRecord into a DynamoDB-style item."""
        return {
            "PK": self._job_pk(job.job_id),
            "SK": "METADATA",
            "entity_type": "JOB",
            "job_id": job.job_id,
            "status": job.status,
            "source_bucket": job.source_bucket,
            "source_key": job.source_key,
            "total_records": job.total_records,
            "valid_records": job.valid_records,
            "invalid_records": job.invalid_records,
            "attempt_count": job.attempt_count,
            "last_error": job.last_error,
            "next_attempt_at": self._optional_datetime_to_string(job.next_attempt_at),
            "created_at": self._datetime_to_string(job.created_at),
            "updated_at": self._datetime_to_string(job.updated_at),
        }

    def _job_from_item(self, item: Mapping[str, Any]) -> JobRecord:
        """Convert a DynamoDB-style item into a JobRecord."""
        return JobRecord(
            job_id=str(item["job_id"]),
            status=str(item["status"]),
            source_bucket=self._optional_str(item.get("source_bucket")),
            source_key=self._optional_str(item.get("source_key")),
            total_records=int(item["total_records"]),
            valid_records=int(item["valid_records"]),
            invalid_records=int(item["invalid_records"]),
            attempt_count=int(item["attempt_count"]),
            last_error=self._optional_str(item.get("last_error")),
            next_attempt_at=self._optional_datetime(item.get("next_attempt_at")),
            created_at=self._required_datetime(item["created_at"]),
            updated_at=self._required_datetime(item["updated_at"]),
        )

    def _processing_error_from_item(self, item: Mapping[str, Any]) -> ProcessingError:
        """Convert a DynamoDB-style item into a ProcessingError."""
        row_number_value = item.get("row_number")
        row_number = None
        if row_number_value is not None:
            row_number = int(row_number_value)

        return ProcessingError(
            error_id=str(item["error_id"]),
            job_id=str(item["job_id"]),
            row_number=row_number,
            error_code=str(item["error_code"]),
            error_message=str(item["error_message"]),
            created_at=self._required_datetime(item["created_at"]),
        )

    def _result_summary_from_item(self, item: Mapping[str, Any]) -> ResultSummary:
        """Convert a DynamoDB-style item into a ResultSummary."""
        return ResultSummary(
            job_id=str(item["job_id"]),
            total_records=int(item["total_records"]),
            valid_records=int(item["valid_records"]),
            invalid_records=int(item["invalid_records"]),
            total_amount=self._optional_decimal(item.get("total_amount")),
            summary=deepcopy(dict(item.get("summary", {}))),
            created_at=self._required_datetime(item["created_at"]),
            updated_at=self._required_datetime(item["updated_at"]),
        )

    def _job_pk(self, job_id: str) -> str:
        """Return the single-table partition key for a job."""
        return f"JOB#{job_id}"

    def _file_gsi_pk(self, bucket: str, object_key_hash: str) -> str:
        """Return the GSI partition key for file idempotency lookup."""
        return f"FILE#{bucket}#{object_key_hash}"

    def _datetime_to_string(self, value: datetime) -> str:
        """Serialize a datetime using a stable UTC ISO format."""
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc).isoformat()

    def _optional_datetime_to_string(self, value: datetime | None) -> str | None:
        """Serialize an optional datetime."""
        if value is None:
            return None

        return self._datetime_to_string(value)

    def _required_datetime(self, value: object) -> datetime:
        """Convert a stored datetime value back into a timezone-aware datetime."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)

            return value

        if isinstance(value, str):
            return datetime.fromisoformat(value)

        raise TypeError(f"Expected datetime value, got {type(value)!r}")

    def _optional_datetime(self, value: object) -> datetime | None:
        """Convert a nullable stored datetime value."""
        if value is None:
            return None

        return self._required_datetime(value)

    def _optional_str(self, value: object) -> str | None:
        """Return a string value or None for nullable attributes."""
        if value is None:
            return None

        return str(value)

    def _decimal_to_string(self, value: Decimal | str | None) -> str | None:
        """Serialize Decimal-compatible values for DynamoDB-style storage."""
        if value is None:
            return None

        return str(value)

    def _optional_decimal(self, value: object) -> Decimal | None:
        """Convert a nullable stored number into Decimal."""
        if value is None:
            return None

        return Decimal(str(value))

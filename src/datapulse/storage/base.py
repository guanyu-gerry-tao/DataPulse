"""Storage backend interface used by handlers and pipeline code."""

from __future__ import annotations

from typing import Mapping
from typing import Protocol

from datapulse.models import DeadLetterMessage
from datapulse.models import FileManifest
from datapulse.models import JobRecord
from datapulse.models import ProcessedRecord
from datapulse.models import ProcessingError
from datapulse.models import ResultSummary


class StorageBackend(Protocol):
    """Common storage contract implemented by concrete backends."""

    def create_job(self, job: JobRecord) -> JobRecord:
        """Persist a new processing job and return the stored record."""
        ...

    def get_job(self, job_id: str) -> JobRecord | None:
        """Return one processing job by id, or None when it does not exist."""
        ...

    def update_job_status(
        self,
        job_id: str,
        status: str,
        metadata: Mapping[str, object] | None = None,
    ) -> JobRecord:
        """Update a job lifecycle status and return the updated record."""
        ...

    def record_file_manifest(self, manifest: FileManifest) -> FileManifest:
        """Persist metadata for one uploaded source file."""
        ...

    def find_job_by_file(self, bucket: str, object_key_hash: str) -> JobRecord | None:
        """Return the job created for one uploaded file, or None when missing."""
        ...

    def save_processed_records(self, job_id: str, records: list[ProcessedRecord]) -> None:
        """Persist structured records for one processed job."""
        ...

    def save_processing_error(self, error: ProcessingError) -> None:
        """Persist one validation or processing error."""
        ...

    def list_processing_errors(self, job_id: str) -> list[ProcessingError]:
        """Return processing errors for one job in creation order."""
        ...

    def save_result_summary(self, summary: ResultSummary) -> None:
        """Persist one result summary for a job."""
        ...

    def get_result_summary(self, job_id: str) -> ResultSummary | None:
        """Return the result summary for one job, or None when missing."""
        ...

    def save_dead_letter_message(self, message: DeadLetterMessage) -> None:
        """Persist evidence for a message that exhausted retries."""
        ...

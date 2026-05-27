"""Storage backend interface used by handlers and pipeline code."""

from __future__ import annotations

from typing import Mapping
from typing import Protocol

from datapulse.models import JobRecord


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

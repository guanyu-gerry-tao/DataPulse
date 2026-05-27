"""Queue adapter interface for DataPulse processing messages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ProcessingMessage:
    """Message that tells a processor which uploaded file to process."""

    job_id: str
    bucket: str
    object_key: str
    object_key_hash: str


class QueueAdapter(Protocol):
    """Common queue contract used by ingestion code."""

    def enqueue(self, message: ProcessingMessage) -> None:
        """Add one processing message to the queue."""
        ...

"""In-memory queue adapter for local tests and development."""

from __future__ import annotations

from datapulse.queue.base import ProcessingMessage
from datapulse.queue.base import QueueAdapter


class InMemoryQueueAdapter(QueueAdapter):
    """Store processing messages in memory for local workflows."""

    def __init__(self) -> None:
        """Create an empty in-memory queue."""
        self.messages: list[ProcessingMessage] = []

    def enqueue(self, message: ProcessingMessage) -> None:
        """Append one processing message to the local queue."""
        self.messages.append(message)

from datapulse.queue.base import ProcessingMessage
from datapulse.queue.memory import InMemoryQueueAdapter


def test_in_memory_queue_records_processing_messages_in_order() -> None:
    queue = InMemoryQueueAdapter()
    first_message = ProcessingMessage(
        job_id="job_001",
        bucket="datapulse-local-raw",
        object_key="uploads/orders.csv",
        object_key_hash="hash-001",
    )
    second_message = ProcessingMessage(
        job_id="job_002",
        bucket="datapulse-local-raw",
        object_key="uploads/customers.json",
        object_key_hash="hash-002",
    )

    queue.enqueue(first_message)
    queue.enqueue(second_message)

    assert queue.messages == [first_message, second_message]

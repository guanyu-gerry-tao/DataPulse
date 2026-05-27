.PHONY: benchmark-query test test-storage

test:
	PYTHONPATH=src python3 -m pytest

test-storage:
	PYTHONPATH=src python3 -m pytest tests/storage_contract

benchmark-query:
	PYTHONPATH=src python3 -m benchmarks.query_benchmark

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

from datapulse.events.s3_event import parse_s3_object_created_event


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_s3_object_created_event_decodes_bucket_and_key() -> None:
    event = json.loads((FIXTURE_DIR / "s3_object_created_event.json").read_text())

    parsed_event = parse_s3_object_created_event(event)

    assert parsed_event.bucket == "datapulse-local-raw"
    assert parsed_event.object_key == "uploads/orders sample.csv"
    assert parsed_event.object_key_hash == sha256(b"uploads/orders sample.csv").hexdigest()
    assert parsed_event.size == 128
    assert parsed_event.etag == "sample-etag-001"


def test_parse_s3_object_created_event_rejects_missing_record() -> None:
    with pytest.raises(ValueError, match="S3 event must contain at least one record"):
        parse_s3_object_created_event({"Records": []})


def test_parse_s3_object_created_event_rejects_non_object_created_event() -> None:
    event = json.loads((FIXTURE_DIR / "s3_object_created_event.json").read_text())
    event["Records"][0]["eventName"] = "ObjectRemoved:Delete"

    with pytest.raises(ValueError, match="S3 event must be ObjectCreated"):
        parse_s3_object_created_event(event)


def test_parse_s3_object_created_event_rejects_multiple_records() -> None:
    event = json.loads((FIXTURE_DIR / "s3_object_created_event.json").read_text())
    event["Records"].append(event["Records"][0])

    with pytest.raises(ValueError, match="S3 event must contain exactly one record"):
        parse_s3_object_created_event(event)

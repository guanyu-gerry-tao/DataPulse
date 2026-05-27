"""Parser for local S3 ObjectCreated-style events."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from typing import Mapping
from urllib.parse import unquote_plus


@dataclass(frozen=True)
class S3ObjectCreatedEvent:
    """Parsed source file information from one S3 ObjectCreated event."""

    bucket: str
    object_key: str
    object_key_hash: str
    size: int | None = None
    etag: str | None = None


def parse_s3_object_created_event(event: Mapping[str, Any]) -> S3ObjectCreatedEvent:
    """Parse the first record from an S3 ObjectCreated-style event."""
    records = event.get("Records")
    if not isinstance(records, list) or len(records) == 0:
        raise ValueError("S3 event must contain at least one record")

    if len(records) > 1:
        raise ValueError("S3 event must contain exactly one record")

    first_record = records[0]
    if not isinstance(first_record, Mapping):
        raise ValueError("S3 event record must be an object")

    event_name = first_record.get("eventName")
    if isinstance(event_name, str) and not event_name.startswith("ObjectCreated:"):
        raise ValueError("S3 event must be ObjectCreated")

    s3_payload = first_record.get("s3")
    if not isinstance(s3_payload, Mapping):
        raise ValueError("S3 event record must contain s3 payload")

    bucket_payload = s3_payload.get("bucket")
    object_payload = s3_payload.get("object")
    if not isinstance(bucket_payload, Mapping) or not isinstance(object_payload, Mapping):
        raise ValueError("S3 event record must contain bucket and object payload")

    bucket = bucket_payload.get("name")
    object_key = object_payload.get("key")
    if not isinstance(bucket, str) or not bucket.strip():
        raise ValueError("S3 event bucket name is required")

    if not isinstance(object_key, str) or not object_key.strip():
        raise ValueError("S3 event object key is required")

    # S3 event keys are URL encoded and use plus signs for spaces.
    decoded_key = unquote_plus(object_key)
    object_key_hash = sha256(decoded_key.encode("utf-8")).hexdigest()

    return S3ObjectCreatedEvent(
        bucket=bucket,
        object_key=decoded_key,
        object_key_hash=object_key_hash,
        size=_optional_int(object_payload.get("size")),
        etag=_optional_str(object_payload.get("eTag")),
    )


def _optional_int(value: object) -> int | None:
    """Convert an optional event value to int."""
    if value is None:
        return None

    return int(value)


def _optional_str(value: object) -> str | None:
    """Convert an optional event value to str."""
    if value is None:
        return None

    return str(value)

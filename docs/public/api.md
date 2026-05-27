# DataPulse Local API Contract

This document describes the local REST-style contract implemented by the M4 API handler. It is designed to map cleanly to API Gateway later, but M4 does not deploy real AWS resources.

## Submit Job

`POST /jobs`

Request body:

```json
{
  "bucket": "datapulse-local-raw",
  "object_key": "uploads/orders.csv",
  "checksum": "etag-001",
  "content_type": "text/csv"
}
```

Success response:

`202 Accepted`

```json
{
  "job_id": "job_abc",
  "status": "QUEUED",
  "created": true,
  "object_key_hash": "..."
}
```

Duplicate file response:

`200 OK`

```json
{
  "job_id": "job_abc",
  "status": "QUEUED",
  "created": false
}
```

## Get Job Status

`GET /jobs/{job_id}`

Success response:

`200 OK`

```json
{
  "job_id": "job_abc",
  "status": "SUCCEEDED",
  "source_bucket": "datapulse-local-raw",
  "source_key": "uploads/orders.csv",
  "total_records": 2,
  "valid_records": 2,
  "invalid_records": 0,
  "attempt_count": 0,
  "last_error": null
}
```

Missing job response:

`404 Not Found`

```json
{
  "message": "Job not found"
}
```

## Get Result Summary

`GET /jobs/{job_id}/result`

Success response:

`200 OK`

```json
{
  "job_id": "job_abc",
  "total_records": 2,
  "valid_records": 2,
  "invalid_records": 0,
  "total_amount": "49.98",
  "summary": {
    "record_type": "order"
  }
}
```

Missing result response:

`404 Not Found`

```json
{
  "message": "Result summary not found"
}
```

## M4 Boundary

The M4 handler is a local API adapter layer. It does not deploy API Gateway, Lambda, IAM, or AWS networking. That deployment work belongs to M5.

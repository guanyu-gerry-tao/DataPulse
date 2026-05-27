CREATE TABLE IF NOT EXISTS file_manifests (
  manifest_id VARCHAR(64) PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  bucket VARCHAR(255) NOT NULL,
  object_key VARCHAR(1024) NOT NULL,
  object_key_hash VARCHAR(128) NOT NULL,
  checksum VARCHAR(255),
  content_type VARCHAR(128),
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_file_manifest_bucket_key_hash (bucket, object_key_hash),
  INDEX idx_file_manifests_job_id (job_id),
  CONSTRAINT fk_file_manifests_job FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

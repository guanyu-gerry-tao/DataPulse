CREATE TABLE IF NOT EXISTS jobs (
  job_id VARCHAR(64) PRIMARY KEY,
  status VARCHAR(32) NOT NULL,
  source_bucket VARCHAR(255),
  source_key VARCHAR(1024),
  total_records INT NOT NULL DEFAULT 0,
  valid_records INT NOT NULL DEFAULT 0,
  invalid_records INT NOT NULL DEFAULT 0,
  attempt_count INT NOT NULL DEFAULT 0,
  last_error TEXT,
  next_attempt_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  INDEX idx_jobs_status_created_at (status, created_at),
  INDEX idx_jobs_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS processed_records (
  record_id VARCHAR(64) PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  row_number INT NOT NULL,
  record_type VARCHAR(64),
  amount DECIMAL(18, 2),
  currency VARCHAR(8),
  payload_json JSON,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_processed_records_job_row (job_id, row_number),
  INDEX idx_processed_records_job_id (job_id),
  INDEX idx_processed_records_type (record_type),
  CONSTRAINT fk_processed_records_job FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

CREATE TABLE IF NOT EXISTS processing_errors (
  error_id VARCHAR(64) PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  row_number INT,
  error_code VARCHAR(64) NOT NULL,
  error_message TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  INDEX idx_processing_errors_job_created_at (job_id, created_at),
  CONSTRAINT fk_processing_errors_job FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

CREATE TABLE IF NOT EXISTS result_summaries (
  job_id VARCHAR(64) PRIMARY KEY,
  total_records INT NOT NULL,
  valid_records INT NOT NULL,
  invalid_records INT NOT NULL,
  total_amount DECIMAL(18, 2),
  summary_json JSON,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  CONSTRAINT fk_result_summaries_job FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_messages (
  message_id VARCHAR(64) PRIMARY KEY,
  job_id VARCHAR(64),
  source_queue VARCHAR(128) NOT NULL,
  payload_json JSON NOT NULL,
  error_message TEXT NOT NULL,
  attempt_count INT NOT NULL,
  created_at DATETIME NOT NULL,
  INDEX idx_dead_letter_messages_job_id (job_id),
  INDEX idx_dead_letter_messages_created_at (created_at)
);

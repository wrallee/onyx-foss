ALTER TABLE ingestion_jobs
    ADD COLUMN cc_pair_id BIGINT REFERENCES connector_credential_pairs(id) ON DELETE CASCADE;

UPDATE ingestion_jobs job
SET cc_pair_id = attempt.cc_pair_id
FROM ingestion_attempts attempt
WHERE attempt.id = job.attempt_id;

ALTER TABLE ingestion_jobs
    ALTER COLUMN cc_pair_id SET NOT NULL;

WITH duplicate_active AS (
    SELECT
        id,
        attempt_id,
        ROW_NUMBER() OVER (
            PARTITION BY cc_pair_id
            ORDER BY CASE WHEN state = 'RUNNING' THEN 0 ELSE 1 END, id
        ) AS active_rank
    FROM ingestion_jobs
    WHERE state IN ('QUEUED', 'RUNNING')
)
UPDATE ingestion_attempts attempt
SET status = 'FAILED',
    error_msg = COALESCE(error_msg, 'Superseded by the active job migration'),
    time_updated = CURRENT_TIMESTAMP
FROM duplicate_active duplicate
WHERE duplicate.active_rank > 1
  AND attempt.id = duplicate.attempt_id
  AND attempt.status IN ('NOT_STARTED', 'IN_PROGRESS');

WITH duplicate_active AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY cc_pair_id
            ORDER BY CASE WHEN state = 'RUNNING' THEN 0 ELSE 1 END, id
        ) AS active_rank
    FROM ingestion_jobs
    WHERE state IN ('QUEUED', 'RUNNING')
)
UPDATE ingestion_jobs job
SET state = 'FAILED',
    last_error = COALESCE(last_error, 'Superseded by the active job migration'),
    updated_at = CURRENT_TIMESTAMP
FROM duplicate_active duplicate
WHERE duplicate.active_rank > 1
  AND job.id = duplicate.id;

CREATE UNIQUE INDEX uq_ingestion_job_active_pair
    ON ingestion_jobs(cc_pair_id)
    WHERE state IN ('QUEUED', 'RUNNING');

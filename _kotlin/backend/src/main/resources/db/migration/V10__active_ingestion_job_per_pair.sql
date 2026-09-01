ALTER TABLE ingestion_jobs
    ADD COLUMN cc_pair_id BIGINT REFERENCES connector_credential_pairs(id) ON DELETE CASCADE;

UPDATE ingestion_jobs job
SET cc_pair_id = (
    SELECT attempt.cc_pair_id
    FROM ingestion_attempts attempt
    WHERE attempt.id = job.attempt_id
);

ALTER TABLE ingestion_jobs
    ALTER COLUMN cc_pair_id SET NOT NULL;

UPDATE ingestion_attempts
SET status = 'FAILED',
    error_msg = COALESCE(error_msg, 'Superseded by the active job migration'),
    time_updated = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT attempt_id
    FROM (
        SELECT
            attempt_id,
            ROW_NUMBER() OVER (
                PARTITION BY cc_pair_id
                ORDER BY CASE WHEN state = 'RUNNING' THEN 0 ELSE 1 END, id
            ) AS active_rank
        FROM ingestion_jobs
        WHERE state IN ('QUEUED', 'RUNNING')
    ) ranked
    WHERE active_rank > 1
)
  AND status IN ('NOT_STARTED', 'IN_PROGRESS');

UPDATE ingestion_jobs
SET state = 'FAILED',
    last_error = COALESCE(last_error, 'Superseded by the active job migration'),
    updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY cc_pair_id
                ORDER BY CASE WHEN state = 'RUNNING' THEN 0 ELSE 1 END, id
            ) AS active_rank
        FROM ingestion_jobs
        WHERE state IN ('QUEUED', 'RUNNING')
    ) ranked
    WHERE active_rank > 1
);

ALTER TABLE ingestion_jobs
    ADD COLUMN active_marker SMALLINT;

UPDATE ingestion_jobs
SET active_marker = 1
WHERE state IN ('QUEUED', 'RUNNING');

ALTER TABLE ingestion_jobs
    ADD CONSTRAINT uq_ingestion_job_active_pair UNIQUE (cc_pair_id, active_marker);

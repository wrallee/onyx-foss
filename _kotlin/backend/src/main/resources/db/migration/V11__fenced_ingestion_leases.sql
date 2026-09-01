ALTER TABLE connector_credential_pairs
    ADD COLUMN ingestion_claim_token UUID,
    ADD COLUMN ingestion_lease_expires_at TIMESTAMPTZ;

ALTER TABLE ingestion_jobs
    ADD COLUMN claim_token UUID,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

CREATE INDEX idx_ingestion_job_claimable
    ON ingestion_jobs(run_after, id)
    WHERE state IN ('QUEUED', 'RUNNING');

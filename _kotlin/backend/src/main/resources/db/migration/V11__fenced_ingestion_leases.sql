ALTER TABLE connector_credential_pairs
    ADD COLUMN ingestion_claim_token UUID;
ALTER TABLE connector_credential_pairs
    ADD COLUMN ingestion_lease_expires_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE ingestion_jobs
    ADD COLUMN claim_token UUID;
ALTER TABLE ingestion_jobs
    ADD COLUMN lease_expires_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_ingestion_job_claimable
    ON ingestion_jobs(state, run_after, id);

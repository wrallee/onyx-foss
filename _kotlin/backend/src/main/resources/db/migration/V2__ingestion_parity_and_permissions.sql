ALTER TABLE indexed_documents
    ADD COLUMN external_access JSONB;

ALTER TABLE ingestion_errors
    ADD COLUMN entity_id VARCHAR(2048),
    ADD COLUMN failed_time_range_start TIMESTAMPTZ,
    ADD COLUMN failed_time_range_end TIMESTAMPTZ;

CREATE TABLE permission_sync_attempts (
    id BIGSERIAL PRIMARY KEY,
    cc_pair_id BIGINT NOT NULL REFERENCES connector_credential_pairs(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'NOT_STARTED',
    error_msg TEXT,
    time_started TIMESTAMPTZ,
    time_finished TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE connector_credential_pairs
    ADD COLUMN last_pruned_at TIMESTAMPTZ;

CREATE UNIQUE INDEX uq_permission_sync_attempt_active
    ON permission_sync_attempts(cc_pair_id)
    WHERE status IN ('NOT_STARTED', 'IN_PROGRESS');

ALTER TABLE permission_sync_attempts
    ADD COLUMN full_exception_trace TEXT,
    ADD COLUMN total_docs_synced INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN docs_with_permission_errors INTEGER NOT NULL DEFAULT 0;

CREATE TABLE permission_sync_staging (
    attempt_id BIGINT NOT NULL REFERENCES permission_sync_attempts(id) ON DELETE CASCADE,
    source_document_id VARCHAR(2048) NOT NULL,
    external_access JSONB NOT NULL,
    has_error BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (attempt_id, source_document_id)
);

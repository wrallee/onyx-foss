ALTER TABLE ingestion_attempts
    ADD COLUMN enumeration_complete BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE ingestion_enumerated_documents (
    attempt_id BIGINT NOT NULL REFERENCES ingestion_attempts(id) ON DELETE CASCADE,
    source_document_id VARCHAR(2048) NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (attempt_id, source_document_id)
);

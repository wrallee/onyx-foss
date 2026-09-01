ALTER TABLE document_set_sync_outbox
    ADD COLUMN locked_at TIMESTAMP WITH TIME ZONE;

CREATE TABLE document_set_sync_claim_lock (
    id SMALLINT PRIMARY KEY,
    CONSTRAINT ck_document_set_sync_claim_lock_singleton CHECK (id = 1)
);

INSERT INTO document_set_sync_claim_lock(id) VALUES (1);

CREATE INDEX idx_document_set_sync_outbox_active
    ON document_set_sync_outbox(status, locked_at, id);

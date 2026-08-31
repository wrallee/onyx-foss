CREATE TABLE document_set_sync_outbox (
    id BIGSERIAL PRIMARY KEY,
    cc_pair_ids JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_document_set_sync_pair_ids_array CHECK (jsonb_typeof(cc_pair_ids) = 'array'),
    CONSTRAINT ck_document_set_sync_status CHECK (status IN ('PENDING', 'IN_PROGRESS', 'DONE'))
);

CREATE INDEX idx_document_set_sync_outbox_pending
    ON document_set_sync_outbox(id)
    WHERE status = 'PENDING';

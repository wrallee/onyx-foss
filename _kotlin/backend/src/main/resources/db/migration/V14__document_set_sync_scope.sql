ALTER TABLE document_set_sync_outbox
    ADD COLUMN document_set_ids JSONB;

ALTER TABLE document_set_sync_outbox
    ADD CONSTRAINT ck_document_set_sync_set_ids_array
        CHECK (document_set_ids IS NULL OR jsonb_typeof(document_set_ids) = 'array');

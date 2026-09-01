ALTER TABLE indexed_documents
    ADD COLUMN primary_owners JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN secondary_owners JSONB NOT NULL DEFAULT '[]'::jsonb;

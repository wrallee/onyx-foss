ALTER TABLE indexed_documents
    ADD COLUMN primary_owners VARCHAR NOT NULL DEFAULT '[]';
ALTER TABLE indexed_documents
    ADD COLUMN secondary_owners VARCHAR NOT NULL DEFAULT '[]';

CREATE TABLE credentials (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(64) NOT NULL,
    name VARCHAR(512),
    secret_json TEXT NOT NULL,
    admin_public BOOLEAN NOT NULL DEFAULT TRUE,
    curator_public BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE connectors (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    source VARCHAR(64) NOT NULL,
    input_type VARCHAR(64) NOT NULL,
    connector_specific_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    refresh_freq BIGINT,
    prune_freq BIGINT,
    indexing_start TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE connector_credential_pairs (
    id BIGSERIAL PRIMARY KEY,
    connector_id BIGINT NOT NULL REFERENCES connectors(id) ON DELETE CASCADE,
    credential_id BIGINT NOT NULL REFERENCES credentials(id) ON DELETE RESTRICT,
    name VARCHAR(512) NOT NULL,
    access_type VARCHAR(32) NOT NULL DEFAULT 'public',
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    auto_sync_options JSONB,
    processing_mode VARCHAR(32) NOT NULL DEFAULT 'REGULAR',
    in_repeated_error_state BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_connector_credential UNIQUE (connector_id, credential_id)
);

CREATE TABLE document_sets (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL UNIQUE,
    description TEXT,
    is_public BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE document_set_cc_pairs (
    document_set_id BIGINT NOT NULL REFERENCES document_sets(id) ON DELETE CASCADE,
    cc_pair_id BIGINT NOT NULL REFERENCES connector_credential_pairs(id) ON DELETE CASCADE,
    PRIMARY KEY (document_set_id, cc_pair_id)
);

CREATE TABLE file_assets (
    id VARCHAR(64) PRIMARY KEY,
    original_name VARCHAR(1024) NOT NULL,
    media_type VARCHAR(256),
    byte_size BIGINT NOT NULL,
    storage_path VARCHAR(2048) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ingestion_attempts (
    id BIGSERIAL PRIMARY KEY,
    cc_pair_id BIGINT NOT NULL REFERENCES connector_credential_pairs(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'NOT_STARTED',
    from_beginning BOOLEAN NOT NULL DEFAULT FALSE,
    new_docs_indexed INTEGER NOT NULL DEFAULT 0,
    total_docs_indexed INTEGER NOT NULL DEFAULT 0,
    docs_removed_from_index INTEGER NOT NULL DEFAULT 0,
    error_msg TEXT,
    full_exception_trace TEXT,
    time_started TIMESTAMPTZ,
    time_updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    poll_range_start TIMESTAMPTZ,
    poll_range_end TIMESTAMPTZ
);

CREATE TABLE ingestion_checkpoints (
    cc_pair_id BIGINT PRIMARY KEY REFERENCES connector_credential_pairs(id) ON DELETE CASCADE,
    checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ingestion_jobs (
    id BIGSERIAL PRIMARY KEY,
    attempt_id BIGINT NOT NULL REFERENCES ingestion_attempts(id) ON DELETE CASCADE,
    state VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    run_after TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(256),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ingestion_jobs_claim ON ingestion_jobs(state, run_after, id);

CREATE TABLE indexed_documents (
    id BIGSERIAL PRIMARY KEY,
    cc_pair_id BIGINT NOT NULL REFERENCES connector_credential_pairs(id) ON DELETE CASCADE,
    source_document_id VARCHAR(2048) NOT NULL,
    title TEXT NOT NULL,
    link TEXT,
    content_hash VARCHAR(128) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_synced TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMPTZ,
    CONSTRAINT uq_indexed_document_source UNIQUE (cc_pair_id, source_document_id)
);

CREATE TABLE ingestion_errors (
    id BIGSERIAL PRIMARY KEY,
    attempt_id BIGINT NOT NULL REFERENCES ingestion_attempts(id) ON DELETE CASCADE,
    source_document_id VARCHAR(2048),
    document_link TEXT,
    failure_message TEXT NOT NULL,
    error_type VARCHAR(128),
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

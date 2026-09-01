ALTER TABLE permission_sync_attempts
    ADD COLUMN claim_token UUID;
ALTER TABLE permission_sync_attempts
    ADD COLUMN lease_expires_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_permission_sync_attempt_claimable
    ON permission_sync_attempts(status, created_at, id);

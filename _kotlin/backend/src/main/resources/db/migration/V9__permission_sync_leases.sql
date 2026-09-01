ALTER TABLE permission_sync_attempts
    ADD COLUMN claim_token UUID,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

CREATE INDEX idx_permission_sync_attempt_claimable
    ON permission_sync_attempts(created_at, id)
    WHERE status IN ('NOT_STARTED', 'IN_PROGRESS');

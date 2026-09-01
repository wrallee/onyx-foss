ALTER TABLE permission_sync_attempts
    ADD COLUMN follow_up_requested BOOLEAN NOT NULL DEFAULT FALSE;

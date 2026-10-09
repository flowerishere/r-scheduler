-- Existing runs retain their original, unbounded lifetime.
ALTER TABLE runs ADD COLUMN expires_at TIMESTAMPTZ;
CREATE INDEX runs_pending_expiry ON runs (expires_at, id)
    WHERE status = 'pending' AND expires_at IS NOT NULL;
CREATE INDEX runs_terminal_history ON runs (finished_at, id)
    WHERE status IN ('succeeded', 'dead', 'cancelled');

ALTER TABLE attempts DROP CONSTRAINT attempts_run_id_fkey;
ALTER TABLE attempts ADD CONSTRAINT attempts_run_id_fkey
    FOREIGN KEY (run_id) REFERENCES runs (id) ON DELETE CASCADE;

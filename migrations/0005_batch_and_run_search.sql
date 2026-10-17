-- Batch idempotency survives schedule edits and execution-history cleanup.
CREATE TABLE schedule_batches (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    schedule_ids UUID[] NOT NULL CHECK (cardinality(schedule_ids) BETWEEN 1 AND 20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

-- Match the immutable run snapshot, independently of later schedule edits.
CREATE INDEX runs_metadata ON runs USING GIN ((spec -> 'metadata') jsonb_path_ops);
CREATE INDEX runs_tenant_scheduled ON runs (tenant_id, scheduled_at, id);

CREATE TABLE schedules (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    spec JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'completed', 'cancelled', 'error')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    next_fire_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    idempotency_key TEXT,
    request_hash TEXT NOT NULL,
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (id, tenant_id)
);

CREATE INDEX schedules_due ON schedules (next_fire_at, id) WHERE status = 'active';
CREATE INDEX schedules_tenant ON schedules (tenant_id, created_at DESC, id DESC);

CREATE TABLE runs (
    id UUID PRIMARY KEY,
    schedule_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'dead', 'cancelled')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    cycle_attempts INTEGER NOT NULL DEFAULT 0 CHECK (cycle_attempts >= 0),
    lease_until TIMESTAMPTZ,
    lease_token UUID,
    last_error TEXT,
    spec JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    UNIQUE (schedule_id, revision, scheduled_at),
    FOREIGN KEY (schedule_id, tenant_id) REFERENCES schedules (id, tenant_id),
    CHECK ((status = 'running') = (lease_until IS NOT NULL AND lease_token IS NOT NULL))
);

CREATE INDEX runs_ready ON runs (available_at, id) WHERE status = 'pending';
CREATE INDEX runs_schedule_ready ON runs (schedule_id, available_at, id) WHERE status = 'pending';
CREATE INDEX runs_expired ON runs (lease_until, id) WHERE status = 'running';
CREATE INDEX runs_schedule ON runs (schedule_id, created_at DESC, id DESC);
CREATE INDEX runs_running_schedule ON runs (schedule_id) WHERE status = 'running';
CREATE INDEX runs_tenant ON runs (tenant_id, created_at DESC, id DESC);

CREATE TABLE attempts (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs (id),
    number INTEGER NOT NULL CHECK (number > 0),
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'lease_expired')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    http_status INTEGER,
    error TEXT,
    response_excerpt TEXT,
    lease_token UUID NOT NULL UNIQUE,
    UNIQUE (run_id, number)
);

-- Limits and arbitration live in PostgreSQL, shared by every worker process.
-- Close the gap between backfilling tenants and installing the insert trigger.
LOCK TABLE schedules IN SHARE ROW EXCLUSIVE MODE;
CREATE SEQUENCE tenant_dispatch_order;
CREATE TABLE tenant_dispatch (
    tenant_id TEXT PRIMARY KEY,
    max_concurrent INTEGER CHECK (max_concurrent BETWEEN 1 AND 4096),
    rate_per_second INTEGER CHECK (rate_per_second BETWEEN 1 AND 10000),
    burst INTEGER NOT NULL DEFAULT 1 CHECK (burst BETWEEN 1 AND 10000),
    tokens DOUBLE PRECISION NOT NULL DEFAULT 1 CHECK (tokens >= 0 AND tokens <= burst),
    token_time TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_served BIGINT NOT NULL DEFAULT 0 CHECK (last_served >= 0)
);
CREATE INDEX tenant_dispatch_turn ON tenant_dispatch (last_served, tenant_id);
INSERT INTO tenant_dispatch (tenant_id) SELECT DISTINCT tenant_id FROM schedules;

-- Register tenants even when schedules are inserted by an older API process.
CREATE FUNCTION scheduler_register_tenant() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO tenant_dispatch (tenant_id) VALUES (NEW.tenant_id) ON CONFLICT DO NOTHING;
    RETURN NEW;
END
$$;
CREATE TRIGGER schedules_register_tenant BEFORE INSERT ON schedules
    FOR EACH ROW EXECUTE FUNCTION scheduler_register_tenant();

CREATE INDEX runs_tenant_ready ON runs (tenant_id, available_at, id)
    WHERE status = 'pending';
CREATE INDEX runs_tenant_running ON runs (tenant_id)
    WHERE status = 'running';

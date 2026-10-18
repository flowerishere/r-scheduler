-- $2 selects the fallback. Keep ordinary turns bounded without enumerating
-- every active tenant; historical empty tenants must not slow the fallback.
WITH RECURSIVE pending_tenants AS (
    (SELECT tenant_id FROM runs
     WHERE $2::boolean AND status = 'pending'
     ORDER BY tenant_id LIMIT 1)
    UNION ALL
    SELECT next.tenant_id FROM pending_tenants previous
    CROSS JOIN LATERAL (
        -- Seek past each tenant's entire backlog using runs_tenant_ready.
        SELECT tenant_id FROM runs
        WHERE status = 'pending' AND tenant_id > previous.tenant_id
        ORDER BY tenant_id LIMIT 1
    ) AS next
), turns AS MATERIALIZED (
    (SELECT tenant_id, last_served FROM tenant_dispatch
     WHERE NOT $2::boolean AND tenant_id <> ALL($1::text[])
     ORDER BY last_served, tenant_id LIMIT 32)
    UNION ALL
    SELECT t.tenant_id, t.last_served FROM pending_tenants p
    JOIN tenant_dispatch t ON t.tenant_id = p.tenant_id
    WHERE t.tenant_id <> ALL($1::text[])
), ordered_turns AS MATERIALIZED (
    -- Sort before probing each tenant's work so LIMIT can stop after the first
    -- eligible tenant instead of inspecting every active tenant's schedules.
    SELECT tenant_id, last_served FROM turns ORDER BY last_served, tenant_id
)
SELECT t.tenant_id
FROM ordered_turns page JOIN tenant_dispatch t ON t.tenant_id = page.tenant_id
CROSS JOIN LATERAL (
    -- Retain ORDER BY/LIMIT so a large tenant backlog can use an index seek,
    -- instead of collecting every matching run into a bitmap for EXISTS.
    SELECT ready.id FROM runs ready JOIN schedules s ON s.id = ready.schedule_id
    WHERE ready.tenant_id = t.tenant_id AND ready.status = 'pending'
      AND ready.available_at <= statement_timestamp()
      AND (ready.expires_at IS NULL OR ready.expires_at > statement_timestamp())
      AND s.status IN ('active', 'completed') AND ready.revision = s.revision
      -- Match ConcurrencyPolicy's Allow default for older stored definitions.
      AND (COALESCE(s.spec->>'concurrency', 'allow') = 'allow' OR NOT EXISTS (
          SELECT 1 FROM runs r WHERE r.schedule_id = s.id AND r.status = 'running'))
    ORDER BY ready.available_at, ready.id LIMIT 1
) AS candidate
WHERE t.tenant_id <> ALL($1::text[])
  AND (t.max_concurrent IS NULL OR
       (SELECT COUNT(*) FROM runs r WHERE r.tenant_id = t.tenant_id AND r.status = 'running') < t.max_concurrent)
  AND (t.rate_per_second IS NULL OR
       LEAST(t.burst::float8, t.tokens + GREATEST(EXTRACT(EPOCH FROM (statement_timestamp() - t.token_time)), 0)::float8 * t.rate_per_second) >= 1)
ORDER BY page.last_served, page.tenant_id
LIMIT 1 FOR UPDATE OF t SKIP LOCKED

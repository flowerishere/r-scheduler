SELECT t.tenant_id
FROM tenant_dispatch t
CROSS JOIN LATERAL (
    -- Retain ORDER BY/LIMIT so a large tenant backlog can use an index seek,
    -- instead of collecting every matching run into a bitmap for EXISTS.
    SELECT ready.id FROM runs ready JOIN schedules s ON s.id = ready.schedule_id
    WHERE ready.tenant_id = t.tenant_id AND ready.status = 'pending'
      AND ready.available_at <= statement_timestamp()
      AND (ready.expires_at IS NULL OR ready.expires_at > statement_timestamp())
      AND s.status IN ('active', 'completed') AND ready.revision = s.revision
      AND (s.spec->>'concurrency' = 'allow' OR NOT EXISTS (
          SELECT 1 FROM runs r WHERE r.schedule_id = s.id AND r.status = 'running'))
    ORDER BY ready.available_at, ready.id LIMIT 1
) AS candidate
WHERE t.tenant_id <> ALL($1::text[])
  AND (t.max_concurrent IS NULL OR
       (SELECT COUNT(*) FROM runs r WHERE r.tenant_id = t.tenant_id AND r.status = 'running') < t.max_concurrent)
  AND (t.rate_per_second IS NULL OR
       LEAST(t.burst::float8, t.tokens + GREATEST(EXTRACT(EPOCH FROM (statement_timestamp() - t.token_time)), 0)::float8 * t.rate_per_second) >= 1)
ORDER BY t.last_served, t.tenant_id
LIMIT 1 FOR UPDATE OF t SKIP LOCKED

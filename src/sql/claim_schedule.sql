SELECT s.* FROM runs ready JOIN schedules s ON s.id = ready.schedule_id
WHERE ready.tenant_id = $1 AND ready.status = 'pending'
  AND ready.available_at <= statement_timestamp()
  AND (ready.expires_at IS NULL OR ready.expires_at > statement_timestamp())
  AND s.status IN ('active', 'completed') AND ready.revision = s.revision
  -- Match ConcurrencyPolicy's Allow default for older stored definitions.
  AND (COALESCE(s.spec->>'concurrency', 'allow') = 'allow' OR NOT EXISTS (
      SELECT 1 FROM runs r WHERE r.schedule_id = s.id AND r.status = 'running'))
ORDER BY ready.available_at, ready.id
LIMIT 1 FOR UPDATE OF s SKIP LOCKED

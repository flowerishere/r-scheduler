WITH candidate AS (
    SELECT id, COALESCE((spec -> 'target' ->> 'timeout_seconds')::bigint, $4::bigint) + 15 AS lease_seconds
    FROM runs
    WHERE schedule_id = $1 AND revision = $2 AND status = 'pending'
      AND available_at <= statement_timestamp()
      AND (expires_at IS NULL OR expires_at > statement_timestamp())
    ORDER BY available_at, id
    LIMIT 1 FOR UPDATE SKIP LOCKED
), leased AS (
    UPDATE runs r
    SET status = 'running', attempt_count = r.attempt_count + 1,
        cycle_attempts = r.cycle_attempts + 1, lease_token = $3,
        lease_until = clock_timestamp() + make_interval(secs => candidate.lease_seconds::double precision),
        finished_at = NULL
    FROM candidate
    WHERE r.id = candidate.id
      -- Admission may expire after the candidate was selected.
      AND (r.expires_at IS NULL OR r.expires_at > clock_timestamp())
    RETURNING r.*
), attempt AS (
    INSERT INTO attempts (id, run_id, number, status, lease_token)
    SELECT $5, id, attempt_count, 'running', lease_token FROM leased
    RETURNING run_id
), admission AS (
    UPDATE tenant_dispatch
    SET last_served = nextval('tenant_dispatch_order'),
        tokens = CASE WHEN rate_per_second IS NULL THEN tokens ELSE
            LEAST(burst::float8, tokens + GREATEST(EXTRACT(EPOCH FROM (statement_timestamp() - token_time)), 0)::float8 * rate_per_second) - 1 END,
        token_time = GREATEST(token_time, statement_timestamp())
    WHERE tenant_id = $6 AND EXISTS (SELECT 1 FROM attempt)
    RETURNING tenant_id
)
-- Follow RETURNING dependencies; sibling CTEs share one database snapshot.
SELECT leased.* FROM leased
JOIN attempt ON attempt.run_id = leased.id
JOIN admission ON admission.tenant_id = leased.tenant_id

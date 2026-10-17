#!/usr/bin/env python3
"""Inspect real dispatch query plans in disposable PostgreSQL; never uses DATABASE_URL.

Outputs EXPLAIN ANALYZE JSON for both tenant discovery paths and schedule
selection, not total service throughput. Example:
python3 scripts/explain_dispatch.py --idle-tenants 10000 --generic-plan > plans.json
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenants", type=int, default=100)
    parser.add_argument("--runs-per-tenant", type=int, default=100)
    parser.add_argument("--idle-tenants", type=int, default=0)
    parser.add_argument("--reverse-turns", action="store_true",
                        help="serve active tenants in reverse tenant-name order")
    parser.add_argument("--generic-plan", action="store_true",
                        help="force PostgreSQL generic plans for typed prepared statements")
    args = parser.parse_args()
    if not (0 <= args.tenants <= 10000 and 1 <= args.runs_per_tenant <= 10000
            and 0 <= args.idle_tenants <= 100000
            and args.tenants * args.runs_per_tenant <= 200000):
        parser.error("tenants must be 0..10000, runs-per-tenant 1..10000, "
                     "and idle-tenants 0..100000, with at most 200000 pending runs total")
    root = Path(__file__).resolve().parent.parent
    container = "scheduler-explain-" + uuid.uuid4().hex[:12]
    owned = False

    def docker(*command, sql=None, check=True):
        return subprocess.run(["docker", *command], input=sql, text=True,
                              capture_output=True, timeout=120, check=check)

    def query(sql):
        return docker("exec", "-i", container, "psql", "-X", "-q", "-tA",
                      "-v", "ON_ERROR_STOP=1", "-U", "scheduler", "-d", "scheduler", sql=sql).stdout.strip()

    def explain(sql, parameter_types, values):
        sql = sql.rstrip().rstrip(";")
        if args.generic_plan:
            # PREPARE, plan mode and EXECUTE must share the same psql session.
            statement = (
                "SET plan_cache_mode = force_generic_plan;\n"
                f"PREPARE dispatch_plan ({', '.join(parameter_types)}) AS {sql};\n"
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                f"EXECUTE dispatch_plan ({', '.join(values)});"
            )
        else:
            # Parameters below are static SQL literals, never shell arguments.
            for n in range(len(values), 0, -1):
                sql = sql.replace(f"${n}", values[n - 1])
            statement = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql
        return json.loads(query(statement))[0]

    try:
        docker("create", "--name", container, "-e", "POSTGRES_USER=scheduler",
               "-e", "POSTGRES_PASSWORD=explain-only-password", "-e", "POSTGRES_DB=scheduler", "postgres:17-alpine")
        owned = True
        docker("start", container)
        for _ in range(120):
            if docker("exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "scheduler", "-d", "scheduler", check=False).returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("PostgreSQL did not become ready")
        for migration in sorted((root / "migrations").glob("*.sql")):
            query("BEGIN;\n" + migration.read_text() + "\nCOMMIT;")
        # Only validated integers are interpolated; all other fixture text is static.
        query(f"""
            INSERT INTO schedules (id, tenant_id, spec, status, request_hash)
            SELECT gen_random_uuid(), 'bench-' || lpad(n::text, 5, '0'),
                '{{"name":"plan fixture","concurrency":"allow","payload":{{}},"trigger":{{"type":"once","at":"2026-01-01T00:00:00Z"}},"target":{{"url":"https://example.invalid/hook"}}}}'::jsonb,
                'completed', 'fixture' FROM generate_series(1, {args.tenants}) n;
            INSERT INTO runs (id, schedule_id, tenant_id, revision, scheduled_at, available_at, status, spec)
            SELECT gen_random_uuid(), s.id, s.tenant_id, 1,
                statement_timestamp() - INTERVAL '1 day' + n * INTERVAL '1 microsecond',
                statement_timestamp() - INTERVAL '1 day' + n * INTERVAL '1 microsecond',
                'pending', s.spec FROM schedules s CROSS JOIN generate_series(1, {args.runs_per_tenant}) n;
            INSERT INTO tenant_dispatch (tenant_id)
            SELECT 'a-idle-' || lpad(n::text, 6, '0') FROM generate_series(1, {args.idle_tenants}) n;
            UPDATE tenant_dispatch SET max_concurrent = 8, rate_per_second = 100, burst = 10, tokens = 10;
        """)
        if args.reverse_turns:
            query(f"""
                UPDATE tenant_dispatch
                SET last_served = {args.tenants} - substring(tenant_id FROM 7)::bigint + 1
                WHERE tenant_id LIKE 'bench-%';
            """)
        query("ANALYZE;")
        tenant_sql = (root / "src" / "sql" / "claim_tenant.sql").read_text()
        schedule_sql = (root / "src" / "sql" / "claim_schedule.sql").read_text()
        fast = explain(tenant_sql, ["text[]", "boolean"], ["ARRAY[]::text[]", "false"])
        plans = {
            "claim_tenant": fast,  # Preserve the original key for existing consumers.
            "claim_tenant_fast": fast,
            "claim_tenant_fallback": explain(
                tenant_sql, ["text[]", "boolean"], ["ARRAY[]::text[]", "true"]),
            "claim_schedule": explain(schedule_sql, ["text"], ["'bench-00001'"]),
        }
        print(json.dumps({"fixture": {
            "tenants": args.tenants,
            "runs_per_tenant": args.runs_per_tenant,
            "idle_tenants": args.idle_tenants,
            "reverse_turns": args.reverse_turns,
            "generic_plan": args.generic_plan,
        }, "plans": plans}, indent=2))
    finally:
        if owned:
            docker("rm", "-fv", container)


if __name__ == "__main__":
    main()

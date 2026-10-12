#!/usr/bin/env python3
"""Inspect real dispatch query plans in disposable PostgreSQL; never uses DATABASE_URL.

Outputs EXPLAIN ANALYZE JSON. This measures the two candidate queries, not total
service throughput. Example: python3 scripts/explain_dispatch.py > plans.json
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
    args = parser.parse_args()
    if not (1 <= args.tenants <= 10000 and 1 <= args.runs_per_tenant <= 10000
            and args.tenants * args.runs_per_tenant <= 200000):
        parser.error("tenants and runs-per-tenant must be 1..10000, with at most 200000 runs total")
    root = Path(__file__).resolve().parent.parent
    container = "scheduler-explain-" + uuid.uuid4().hex[:12]
    owned = False

    def docker(*command, sql=None, check=True):
        return subprocess.run(["docker", *command], input=sql, text=True,
                              capture_output=True, timeout=120, check=check)

    def query(sql):
        return docker("exec", "-i", container, "psql", "-X", "-q", "-tA",
                      "-v", "ON_ERROR_STOP=1", "-U", "scheduler", "-d", "scheduler", sql=sql).stdout.strip()

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
            UPDATE tenant_dispatch SET max_concurrent = 8, rate_per_second = 100, burst = 10, tokens = 10;
            ANALYZE;
        """)
        plans = {}
        for name, parameter in [("claim_tenant", "ARRAY[]::text[]"), ("claim_schedule", "'bench-00001'")]:
            sql = (root / "src" / "sql" / f"{name}.sql").read_text().replace("$1", parameter)
            plans[name] = json.loads(query("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql))[0]
        print(json.dumps({"fixture": {"tenants": args.tenants, "runs_per_tenant": args.runs_per_tenant}, "plans": plans}, indent=2))
    finally:
        if owned:
            docker("rm", "-fv", container)


if __name__ == "__main__":
    main()

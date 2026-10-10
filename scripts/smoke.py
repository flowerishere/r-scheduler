#!/usr/bin/env python3
"""Exercise an existing service image with disposable PostgreSQL and real HTTP.

Requires Docker and Python 3, no Python packages. Only resources created by this
script are removed. Example: python3 scripts/smoke.py --image scheduler-service:local
"""
import argparse
import datetime as dt
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def docker(*args, check=True):
    return subprocess.run(
        ["docker", *args], check=check, text=True, capture_output=True, timeout=60
    )


def eventually(predicate, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError("Timed out waiting for expected state")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="scheduler-service:local")
    args = parser.parse_args()
    docker("image", "inspect", args.image)
    prefix = f"scheduler-smoke-{uuid.uuid4().hex[:12]}"
    database, service = f"{prefix}-db", f"{prefix}-service"
    owned_containers = []
    network_created = False
    requests, lock = [], threading.Lock()

    class Callback(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with lock:
                requests.append((time.monotonic(), self.headers["Idempotency-Key"], body))
                count = len(requests)
            self.send_response(503 if count == 1 else 200)
            self.send_header("Retry-After", "2")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *_):
            pass

    receiver = ThreadingHTTPServer(("0.0.0.0", 0), Callback)
    thread = threading.Thread(target=receiver.serve_forever, daemon=True)
    thread.start()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        docker("network", "create", prefix)
        network_created = True
        owned_containers.append(database)
        docker("run", "-d", "--name", database, "--network", prefix,
               "--network-alias", "postgres", "-e", "POSTGRES_USER=scheduler",
               "-e", "POSTGRES_PASSWORD=smoke-only-password",
               "-e", "POSTGRES_DB=scheduler", "postgres:17-alpine")
        eventually(lambda: docker("exec", database, "pg_isready", "-h", "127.0.0.1", "-U", "scheduler",
                                  "-d", "scheduler", check=False).returncode == 0)
        owned_containers.append(service)
        docker("run", "-d", "--init", "--name", service, "--network", prefix,
               "--add-host", "host.docker.internal:host-gateway", "-p", "127.0.0.1::8080",
               "-e", "DATABASE_URL=postgres://scheduler:smoke-only-password@postgres/scheduler",
               "-e", 'SCHEDULER_API_KEYS={"smoke":"smoke-tenant-a-api-key","other":"smoke-tenant-b-api-key"}',
               "-e", "SCHEDULER_ALLOW_PRIVATE_TARGETS=true", "-e", "SCHEDULER_WORKERS=2",
               "-e", "SCHEDULER_POLL_MS=50", args.image)
        port = json.loads(docker("inspect", service).stdout)[0]["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"]
        base = f"http://127.0.0.1:{port}"

        def api(path, body=None, key="smoke-tenant-a-api-key", idempotency=None):
            headers = {"Content-Type": "application/json"}
            if key:
                headers["Authorization"] = f"Bearer {key}"
            if idempotency:
                headers["Idempotency-Key"] = idempotency
            data = json.dumps(body).encode() if body is not None else None
            request = urllib.request.Request(base + path, data=data, headers=headers)
            with opener.open(request, timeout=5) as response:
                raw = response.read().decode()
                return json.loads(raw) if "application/json" in response.headers.get("Content-Type", "") else raw

        def ready():
            try:
                return api("/ready")["status"] == "ready"
            except (urllib.error.URLError, ConnectionError):
                return False

        eventually(ready)
        assert api("/health")["status"] == "ok"
        assert api("/")["version"] == "0.5.0"
        preview = api("/v1/preview", {
            "trigger": {"type": "rrule", "value": "DTSTART;TZID=Asia/Shanghai:20260930T090000\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1\nEXDATE;TZID=Asia/Shanghai:20261030T090000"},
            "after": "2026-09-21T00:00:00Z", "count": 3,
        })
        assert preview["dates"] == ["2026-09-30T01:00:00Z", "2026-11-30T01:00:00Z", "2026-12-31T01:00:00Z"]
        job = {"name": "container smoke", "trigger": {"type": "delay", "seconds": 1},
               "target": {"url": f"http://host.docker.internal:{receiver.server_port}/hook"},
               "retry": {"max_attempts": 3, "initial_delay_seconds": 1,
                         "max_delay_seconds": 5, "max_age_seconds": 120}}
        created = api("/v1/schedules", job, idempotency="smoke-job")
        assert api("/v1/schedules", job, idempotency="smoke-job")["id"] == created["id"]
        def finished(schedule_id, status):
            runs = api(f"/v1/runs?schedule_id={schedule_id}")
            return runs[0] if runs and runs[0]["status"] == status else None

        run = eventually(lambda: finished(created["id"], "succeeded"))
        attempts = api(f"/v1/runs/{run['id']}/attempts")
        assert [a["http_status"] for a in attempts] == [503, 200]
        with lock:
            assert len(requests) == 2
            assert requests[0][1] == requests[1][1] == run["id"]
            assert requests[1][0] - requests[0][0] >= 2
        assert 'scheduler_runs{status="succeeded"} 1\n' in api("/v1/metrics")
        assert 'scheduler_runs{status="succeeded"} 0\n' in api("/v1/metrics", key="smoke-tenant-b-api-key")
        try:
            api("/v1/metrics", key=None)
            raise AssertionError("Metrics accepted an unauthenticated request")
        except urllib.error.HTTPError as error:
            assert error.code == 401

        job["trigger"] = {"type": "once", "at": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()}
        job["retry"]["max_age_seconds"] = 1
        expired = api("/v1/schedules", job)
        dead = eventually(lambda: finished(expired["id"], "dead"))
        assert dead["attempt_count"] == 0
        with lock:
            assert len(requests) == 2
        report = json.loads(docker("exec", service, "scheduler-service", "cleanup").stdout)
        assert report["dry_run"] and report["deleted_runs"] == 0
        docker("stop", "--time", "10", service)
        state = json.loads(docker("inspect", service).stdout)[0]["State"]
        assert state["ExitCode"] == 0, state
        print("PASS: image startup, migration, RRULE preview, delayed callback, Retry-After, expired run, cleanup preview, tenant metrics, stable idempotency key, graceful shutdown")
    except BaseException:
        if service in owned_containers:
            logs = docker("logs", "--tail", "80", service, check=False)
            print(logs.stdout + logs.stderr)
        raise
    finally:
        receiver.shutdown()
        receiver.server_close()
        thread.join(timeout=2)
        for name in reversed(owned_containers):
            docker("rm", "-fv", name, check=False)
        if network_created:
            docker("network", "rm", prefix, check=False)


if __name__ == "__main__":
    main()

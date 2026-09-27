#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

# An existing DATABASE_URL must point at a development PostgreSQL instance whose
# user can CREATE DATABASE. sqlx::test gives each test its own isolated database.
test_container=""
cleanup() {
    if [[ -n "$test_container" ]]; then
        docker rm -fv "$test_container" >/dev/null
    fi
}
trap cleanup EXIT

if [[ -z "${DATABASE_URL:-}" ]]; then
    test_container="scheduler-tests-$$"
    docker run -d --name "$test_container" \
        -e POSTGRES_USER=scheduler -e POSTGRES_PASSWORD=test-only-password \
        -e POSTGRES_DB=scheduler -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
    for _ in {1..60}; do
        if docker exec "$test_container" pg_isready -h 127.0.0.1 -U scheduler -d scheduler >/dev/null 2>&1; then break; fi
        sleep 1
    done
    docker exec "$test_container" pg_isready -h 127.0.0.1 -U scheduler -d scheduler >/dev/null
    test_port="$(docker inspect -f '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$test_container")"
    export DATABASE_URL="postgres://scheduler:test-only-password@127.0.0.1:${test_port}/scheduler"
fi

cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets -- --include-ignored

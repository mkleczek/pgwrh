#!/usr/bin/env bash
# Run as an unprivileged user with the packaged PostgreSQL binaries on PATH.
set -euo pipefail
sql=${1:-$(dirname "$0")/installed.sql}
test_root=$(mktemp -d)
cleanup() {
    pg_ctl -D "$test_root/data" -m immediate -w stop >/dev/null 2>&1 || true
    rm -rf "$test_root"
}
trap cleanup EXIT
initdb -D "$test_root/data" -U postgres -A trust --no-locale >/dev/null
cat >> "$test_root/data/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$test_root'
shared_preload_libraries = 'pgwrh_wait'
wal_level = logical
max_worker_processes = 32
CONF
if ! pg_ctl -D "$test_root/data" -l "$test_root/postgres.log" -w start; then
    cat "$test_root/postgres.log"
    exit 1
fi
psql -X -h "$test_root" -U postgres -d postgres -v ON_ERROR_STOP=1 -f "$sql"

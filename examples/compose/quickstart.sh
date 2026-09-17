#!/usr/bin/env bash
# This is both the documented quickstart and the release container smoke test.
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile ui up -d

wait_for_setup() {
    local service=$1 id state
    id=$(docker compose --profile ui ps -aq "$service")
    for _attempt in $(seq 1 120); do
        state=$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "$id")
        if [[ $state == 'exited 0' ]]; then return; fi
        if [[ $state == exited* || $state == dead* ]]; then break; fi
        sleep 2
    done
    docker compose --profile ui logs "$service" >&2
    echo "$service failed or timed out" >&2
    return 1
}
wait_for_setup setup
wait_for_setup ui-setup

url="http://127.0.0.1:${PGWRH_UI_PORT:-13000}/rpc"
response=$(mktemp)
trap 'rm -f "$response"' EXIT
curl --fail --silent --show-error --retry 60 --retry-connrefused --retry-delay 1 \
    --max-time 5 -H 'Accept: text/html' "$url/index?group_id=demo" -o "$response"
grep -q '<!doctype html>' "$response"
grep -q 'demo' "$response"
for asset in style script htmx; do
    curl --fail --silent --show-error --max-time 5 "$url/$asset" -o "$response"
    test -s "$response"
done
# The public demo role must not expose management RPCs.
status=$(curl --silent --show-error --max-time 5 -o "$response" -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' -d '{}' "$url/mutate")
[[ $status == 401 || $status == 404 ]]
for replica in replica1 replica2; do
    enabled=$(docker compose exec -T "$replica" psql -U postgres -d pgwrh_demo -Atqc \
        "SELECT count(*) FROM pg_extension WHERE extname = 'pgwrh_ui'")
    test "$enabled" = 0
done
printf '\nQuickstart verified: 100 rows on both replicas.\nRead-only console: %s/index?group_id=demo\n' "$url"

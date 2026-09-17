#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
psql -X -h controller -v ON_ERROR_STOP=1 -f seed.sql
for replica in replica1 replica2; do
    psql -X -h "$replica" -v ON_ERROR_STOP=1 -v replica="$replica" -v password="${replica}_demo" <<'SQL'
SELECT NOT EXISTS (SELECT FROM pg_subscription WHERE subname = 'pgwrh_replica_subscription') AS configure \gset
\if :configure
SELECT pgwrh.configure_controller('controller', '5432', :'replica', :'password', refresh_seconds := 0.5);
\else
SELECT pgwrh.start_sync_daemon(0.5);
\endif
SQL
done
psql -X -h controller -v ON_ERROR_STOP=1 -c "SELECT pgwrh.start_rollout('demo')"
# Poll the same readiness contract as the integration tests, with a fixed deadline.
ready=false
for _attempt in $(seq 1 120); do
    if [[ $(psql -X -h controller -At -v ON_ERROR_STOP=1 <<'SQL'
WITH missing AS (
    SELECT replication_group_id, version FROM pgwrh.missing_subscribed_shard
    UNION ALL SELECT replication_group_id, version FROM pgwrh.missing_connected_local_shard
    UNION ALL SELECT replication_group_id, version FROM pgwrh.missing_ready_remote_shard
)
SELECT NOT EXISTS (SELECT FROM missing m JOIN pgwrh.replication_group g
    ON m.replication_group_id = g.replication_group_id AND m.version = g.target_version
    WHERE g.replication_group_id = 'demo');
SQL
    ) == t ]]; then ready=true; break; fi
    sleep 1
done
if [[ $ready != true ]]; then
    echo 'Demo rollout did not become ready; inspect the controller and replica logs.' >&2
    exit 1
fi
psql -X -h controller -v ON_ERROR_STOP=1 -c "SELECT pgwrh.commit_rollout('demo')"
bash verify.sh
printf '\npgwrh demo ready: query demo.events on either replica (100 rows).\n'

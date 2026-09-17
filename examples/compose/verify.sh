#!/usr/bin/env bash
set -euo pipefail
query="SELECT count(*) || ':' || sum(id) || ':' || md5(string_agg(message, ',' ORDER BY id)) FROM demo.events"
expected=$(psql -X -h controller -At -v ON_ERROR_STOP=1 -c "$query")
[[ $expected == 100:5050:* ]]
for replica in replica1 replica2; do
    matched=false
    for _attempt in $(seq 1 60); do
        if actual=$(psql -X -h "$replica" -At -v ON_ERROR_STOP=1 -c "$query") && [[ $actual == "$expected" ]]; then
            matched=true
            break
        fi
        sleep 1
    done
    if [[ $matched != true ]]; then
        echo "$replica did not match the controller's rows" >&2
        exit 1
    fi
    printf '%s: %s\n' "$replica" "$actual"
done

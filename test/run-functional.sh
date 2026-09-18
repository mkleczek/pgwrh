#!/usr/bin/env bash
# Run the same complete integration suites in CI and from release archives.
set -euo pipefail
cd "$(dirname "$0")/.."
export POSTGREST_BIN
POSTGREST_BIN=$(command -v postgrest)
export PGWRH_REQUIRE_HTTP=1
make clean
make testgres-ext test-stage
mkdir -p .build/test-results
python3 -m pytest test/pgwrh test/pgwrh_ui -ra --junitxml=.build/test-results/core-ui.xml
python3 -m pytest test/pgwrh_wait -ra --junitxml=.build/test-results/wait.xml
python3 - <<'CHECK'
from pathlib import Path
import xml.etree.ElementTree as ET
for result in Path('.build/test-results').glob('*.xml'):
    tree = ET.parse(result)
    skipped = tree.findall('.//testcase/skipped')
    if skipped:
        raise SystemExit(f'{result}: {len(skipped)} release tests were skipped')
CHECK

#!/usr/bin/env bash
# Build ABI-matched extensions and run real mixed-major clusters.
set -euo pipefail
cd "$(dirname "$0")/.."

# Validate the complete matrix before building anything. Never fall back to
# PATH or the single-major fixture variables for an absent installation.
for major in 18 19; do
  config_var="PG_CONFIG_$major"
  bin_var="PGWRH_TEST_BIN_DIR_$major"
  : "${!config_var:?Set PG_CONFIG_18 and PG_CONFIG_19 (or use nix develop .#tests-mixed)}"
  : "${!bin_var:?Set PGWRH_TEST_BIN_DIR_18 and PGWRH_TEST_BIN_DIR_19}"
  for tool in "${!config_var}" "${!bin_var}/postgres"; do
    version=$("$tool" --version)
    if [[ ! "$version" =~ PostgreSQL\)?[[:space:]]+$major([^0-9]|$) ]]; then
      echo "Expected PostgreSQL $major from $tool, got: $version" >&2
      exit 1
    fi
  done
done

for major in 18 19; do
  config_var="PG_CONFIG_$major"
  ext_var="PGWRH_TEST_EXT_PATHS_$major"
  printf -v "$ext_var" '%s' "$PWD/.build/mixed-versions/$major"
  export "$ext_var"
  # FDW source directories are per-major, but a previous build may have used
  # another installation of that major. Do not remove the other major's stage.
  make -C pgwrh_fdw clean PG_CONFIG="${!config_var}"
  make testgres-ext PG_CONFIG="${!config_var}" TESTGRES_EXT_ROOT="${!ext_var}"
done

mkdir -p .build/test-results
python3 -m pytest test/pgwrh/mixed_versions -v -ra \
  --junitxml=.build/test-results/mixed-versions.xml
python3 - <<'CHECK'
import xml.etree.ElementTree as ET
tree = ET.parse('.build/test-results/mixed-versions.xml')
if tree.findall('.//testcase/skipped'):
    raise SystemExit('Mixed-version tests must not be skipped')
CHECK

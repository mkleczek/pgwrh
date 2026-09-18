#!/usr/bin/env bash
set -euo pipefail
ui_share=$(pg_config --sharedir)/pgwrh_ui
psql -X -v ON_ERROR_STOP=1 -v readonly="$ui_share/readonly.sql" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pgwrh_ui;
SELECT NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgwrh_ui_viewer') AS create_viewer \gset
\if :create_viewer
\i :readonly
\endif
SELECT NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgwrh_ui_authenticator') AS create_authenticator \gset
\if :create_authenticator
CREATE ROLE pgwrh_ui_authenticator NOINHERIT LOGIN PASSWORD 'pgwrh-ui-local-demo';
\endif
GRANT pgwrh_ui_viewer TO pgwrh_ui_authenticator;
SQL

-- SPDX-License-Identifier: AGPL-3.0-or-later
\echo Use "CREATE EXTENSION pgwrh_ui" to load this file. \quit

-- All application objects belong to this extension. No controller tables,
-- triggers, grants, or replica-side objects are changed by installation.
CREATE DOMAIN "text/html" AS text;

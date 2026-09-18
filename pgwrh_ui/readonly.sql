-- Run as a database administrator after CREATE EXTENSION pgwrh_ui.
-- This role has no access to controller tables or UI helper functions.
CREATE ROLE pgwrh_ui_viewer NOLOGIN;
GRANT USAGE ON SCHEMA pgwrh_ui TO pgwrh_ui_viewer;
GRANT EXECUTE ON FUNCTION pgwrh_ui.index(text,text,text,int),
    pgwrh_ui.status(text,text,text,int), pgwrh_ui.style(), pgwrh_ui.script(), pgwrh_ui.htmx()
    TO pgwrh_ui_viewer;

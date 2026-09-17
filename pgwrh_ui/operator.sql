-- Run after readonly.sql. The operator can manage every group on this controller.
CREATE ROLE pgwrh_ui_operator NOLOGIN;
GRANT pgwrh_ui_viewer TO pgwrh_ui_operator;
GRANT EXECUTE ON FUNCTION pgwrh_ui.mutate(text,text,text,text,text,text,int,text,int,boolean,boolean)
    TO pgwrh_ui_operator;

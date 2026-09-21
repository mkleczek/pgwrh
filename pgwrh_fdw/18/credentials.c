/* Managed credentials use PostgreSQL's own SCRAM implementation and policy. */
#include "postgres.h"

#include "fmgr.h"
#include "libpq/scram.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(pgwrh_fdw_scram_verifier);

Datum
pgwrh_fdw_scram_verifier(PG_FUNCTION_ARGS)
{
	char *password = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *verifier = pg_be_scram_build_secret(password);

	PG_RETURN_TEXT_P(cstring_to_text(verifier));
}

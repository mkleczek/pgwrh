# Upstream provenance and patch workflow

Source: https://github.com/postgres/postgres

| PostgreSQL | Source tag | Original commit | Pristine bookmark | Patched aggregate | Directory in main |
| --- | --- | --- | --- | --- | --- |
| 18.3 | `REL_18_3` | `62d6c7d3df6287f1bd83199c1a746e50d31571a0` | `upstream/postgres_fdw` | `fdw_base_18` | `pgwrh_fdw/18/` |
| 19 Beta 3 (preview) | `REL_19_BETA3` | `3638289fb57bdabec00deda98ee9624a35f5d66a` | `upstream/postgres_fdw_19` | `fdw_base_19` | `pgwrh_fdw/19/` |

Each pristine bookmark contains unmodified PostgreSQL history filtered to
`contrib/postgres_fdw`. The annotated `upstream/REL_*` tags record the original
full-repository commit and the filtered path. Each directory's `COPYRIGHT`
comes from its matching PostgreSQL release.

## History layout

The functional patches apply to the filtered tree at the repository root,
retaining upstream C/header filenames and regression-test paths. Each change
contains its implementation and tests. Dependencies remain explicit; the SCRAM
verifier change is independent of the routing changes.

Each `fdw_base_MAJOR` bookmarks one aggregate with all 13 functional changes as
direct parents. The aggregate resolves the shared build, SQL and export lists.
It contains no pgwrh directory moves or import tooling. The PostgreSQL 19 graph
was copied from the 18 graph and adapted within the affected patches.

Non-squashed Git subtree imports place these exact aggregates under
`pgwrh_fdw/18/` and `pgwrh_fdw/19/`. Tests stay inside each imported tree.
The common `pgwrh_fdw/Makefile` selects the directory using `PG_CONFIG` and
excludes the test-only `context_probe` module from installed packages.

There is one `main` and one pgwrh release version. SQL, UI and `pgwrh_wait`
remain shared; the wait extension isolates PostgreSQL API differences in
`pgwrh_wait/src/compat.h`. Existing release ancestry remains intact.

```sh
jj log -r 'upstream/postgres_fdw..fdw_base_18'
jj log -r 'upstream/postgres_fdw_19..fdw_base_19'
jj log -r 'parents(fdw_base_19)'
```

Functional patches can be reviewed or exported without removing a directory
prefix. Proposals for PostgreSQL itself still need to select relevant changes
and adapt the extension-specific API and tests.

## Updating one major

Record that major's current pristine commit before importing. Fetch the selected
release into a separate PostgreSQL checkout, then run from the pgwrh root:

```sh
jj log -r upstream/postgres_fdw_19 --no-graph -T 'commit_id ++ "\n"'
python3 pgwrh_fdw/tools/import-upstream.py /path/to/postgres REL_19_BETA3
```

Use a **new** selected release tag; the example above is already imported.
The helper accepts `REL_18_N`, `REL_19_N`, beta and RC tags. It filters history in
a disposable clone and atomically updates only that major's pristine bookmark
and new annotated tag. Existing tags and unrelated histories are rejected.
Patched aggregates, `main` and working files remain untouched.

Keep `git filter-branch --subdirectory-filter contrib/postgres_fdw` as the
extraction method so filtered history retains compatible ancestry. The first
import of a major creates its pristine bookmark; subsequent imports must be
fast-forwards.

Copy the functional changes **and their aggregate** onto the new pristine base:

```sh
jj duplicate 'OLD_UPSTREAM_ID..fdw_base_19' -o upstream/postgres_fdw_19
```

Resolve conflicts in the copied changes where they originate, including the
aggregate. Verify the standalone build and tests, then advance the aggregate:

```sh
jj bookmark set fdw_base_19 -r NEW_AGGREGATE_ID
```

Use a disposable Git checkout of `main` to integrate the reviewed aggregate:

```sh
git subtree merge --prefix=pgwrh_fdw/19 NEW_AGGREGATE_COMMIT_ID
```

Fetch the resulting commit back into the jj repository and advance `main` after
validation. Do not Git-checkout over an active jj working copy or ordinarily
jj-merge the root-level FDW tree into `main`; the layouts differ. For PostgreSQL
18, use `fdw_base_18`, `upstream/postgres_fdw` and `pgwrh_fdw/18` throughout.

Review upstream SQL and tests alongside C changes. Refresh the provenance table,
`COPYRIGHT` and matching source pins in the FDW CI and Nix preview definition.
The imported directory must match the aggregate exactly.

## Validation and release

Select a matching `PG_CONFIG` for each major and run:

```sh
python3 test/pgwrh_fdw/test_upstream_import.py
make test-fdw
PG_SOURCE=/path/to/matching/postgres/source make test-fdw-tap
make test-packaging
make test-wait
make test-pgwrh
make test-ui
```

CI runs the shared integration suites and independent FDW suites for both majors.
The importer tests exercise preservation of the other major, dirty working files,
immutable provenance tags, unrelated-history rejection and later subtree updates.
Publish the pristine and aggregate bookmarks and annotated upstream tags together
with the reviewed integration. See [PostgreSQL compatibility](../docs/development/postgres-versions.md)
for the preview-to-release checklist.

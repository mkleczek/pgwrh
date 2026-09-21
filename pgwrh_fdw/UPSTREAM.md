# Upstream provenance and patch workflow

Source: https://github.com/postgres/postgres

Current imported release: PostgreSQL 18.3, tag `REL_18_3`.
Original commit: `62d6c7d3df6287f1bd83199c1a746e50d31571a0`.
Filtered commit: `6ba739cca5fb00282732191c46a61d3408836284`.

## History layout

`upstream/postgres_fdw` contains unmodified PostgreSQL 18 history filtered to
`contrib/postgres_fdw`. Its files are at the repository root.

The local functional changes build on that filtered history, retaining upstream
C/header filenames and regression-test paths. Each change contains its related
implementation and tests. Dependencies remain explicit; the SCRAM verifier
change is independent of the routing changes.

`fdw_base_18` bookmarks one integration change with **all 13 functional changes
as direct parents**. It represents the complete patched upstream tree, still at
the repository root. The aggregate resolves the shared build, SQL and symbol
lists. It contains no pgwrh repository directory moves or import tooling.

A non-squashed Git subtree import places that exact aggregate under
`pgwrh_fdw/`. A following change moves the tests to `test/pgwrh_fdw/` and adapts
the build paths and project documentation. `main` includes these integration
changes and preserves the released `v1.0.0-alpha1` and all its existing parents.
The earlier standalone release `v0.1.0` also remains in history.

The installed extension, SQL API, GUCs and linker namespace remain `pgwrh_fdw`.
This reorganization keeps the PostgreSQL source at 18.3.
`pgwrh_fdw/COPYRIGHT` comes from that upstream release.

Inspect the functional changes and their aggregate:

```sh
jj log -r 'upstream/postgres_fdw..fdw_base_18'
jj log -r 'parents(fdw_base_18)'
```

The functional changes can be reviewed or exported without stripping the
`pgwrh_fdw/` repository prefix. Proposals for PostgreSQL itself still need to
select the relevant changes and adapt the extension-specific API and tests.

## Importing a later PostgreSQL 18 release

Record the current upstream commit ID before importing. Fetch the selected
release into a separate PostgreSQL Git checkout, then run from the pgwrh root:

```sh
jj log -r upstream/postgres_fdw --no-graph -T 'commit_id ++ "\n"'
python3 pgwrh_fdw/tools/import-upstream.py /path/to/postgres REL_18_N
```

Replace `REL_18_N` with the selected tag. The helper uses
`git filter-branch --subdirectory-filter contrib/postgres_fdw` in a disposable
clone. Keep this extraction method so the filtered history retains compatible
ancestry. It atomically advances only `upstream/postgres_fdw` and creates an
annotated `upstream/REL_18_N` tag recording the original full-repository commit.
It rejects unrelated history and existing tags. It leaves `fdw_base_18`, the
functional changes, `main` and working files untouched.

Duplicate the functional changes **and their aggregate** onto the new upstream
base. This preserves the versions already imported into pgwrh and any releases
that include them:

```sh
jj duplicate 'OLD_UPSTREAM_ID..fdw_base_18' -o upstream/postgres_fdw
```

Replace `OLD_UPSTREAM_ID` with the ID recorded before the import. Resolve
conflicts in the copied changes, including the aggregate, and verify them before
moving the bookmark to the new aggregate ID printed by jj:

```sh
jj bookmark set fdw_base_18 -r NEW_AGGREGATE_ID
```

Use a disposable Git checkout based on `main` for the repository integration.
There, import the reviewed aggregate with:

```sh
git subtree merge --prefix=pgwrh_fdw NEW_AGGREGATE_COMMIT_ID
```

Use the aggregate's Git commit ID, available through `jj log -r fdw_base_18`.
Fetch the resulting integration commit back into this repository, inspect it
with jj, and advance `main` after validation. Do not perform a Git checkout or
merge over the active jj working copy. Do not ordinarily jj-merge the rooted FDW
aggregate into `main`: the source trees have different layouts.

Review upstream SQL and test changes alongside C changes. Relocated tests may
need conflict resolution under `test/pgwrh_fdw/`; new upstream test files may
need moving there. Keep these layout adaptations after the functional patches.
Update the provenance above, `COPYRIGHT` if needed, and the PostgreSQL source
pin in `.github/workflows/fdw.yml`.

## Porting the patches to another PostgreSQL major

Import that major's unmodified history with the same filtering method into a
separate bookmark, for example `upstream/postgres_fdw_17`. Keep the PostgreSQL 18
bookmark and aggregate intact. Copy the same graph onto the other baseline:

```sh
jj duplicate 'upstream/postgres_fdw..fdw_base_18' -o upstream/postgres_fdw_17
jj bookmark create fdw_base_17 -r NEW_AGGREGATE_ID
```

This preserves patch dependencies and the single aggregate. Apply PostgreSQL
API compatibility fixes to the copied changes and test against that major.
The functional patches carry no subtree paths or import-tool configuration;
the repository layout is applied only when importing the resulting aggregate.
Portability of the patch history does not imply binary or source compatibility
without those checks. The current import helper and CI target PostgreSQL 18.

## Validation

Run the standalone FDW build and its tests at the aggregate before importing it.
After repository integration, run:

```sh
python3 test/pgwrh_fdw/test_upstream_import.py
make test-fdw
PG_SOURCE=/path/to/matching/postgres/source make test-fdw-tap
make test-packaging
make test-wait
make test-pgwrh
```

The importer tests use synthetic repositories to check that the patched
aggregate stays unchanged and that refreshed patches can be subtree-merged.
They do not update the project's real upstream baseline. Publish the filtered
upstream and `fdw_base_18` bookmarks and annotated upstream tags together with
the reviewed repository integration.

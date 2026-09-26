# Upstream provenance and patch workflow

Source: https://github.com/postgres/postgres

| PostgreSQL | Source tag | Original commit | Pristine bookmark | Aggregate | Directory-move bookmark |
| --- | --- | --- | --- | --- | --- |
| 18.3 | `REL_18_3` | `62d6c7d3df6287f1bd83199c1a746e50d31571a0` | `upstream/postgres_fdw` | `fdw_base_18-` | `fdw_base_18` |
| 19 Beta 3 (preview) | `REL_19_BETA3` | `3638289fb57bdabec00deda98ee9624a35f5d66a` | `upstream/postgres_fdw_19` | `fdw_base_19-` | `fdw_base_19` |

Each pristine bookmark contains unmodified PostgreSQL history filtered to
`contrib/postgres_fdw`. The annotated `upstream/REL_*` tags record the original
full-repository commit and the filtered path. Each directory's `COPYRIGHT`
comes from its matching PostgreSQL release.

## History layout

The functional changes form one shared graph rooted at `fdw_patch_base`,
the common upstream ancestor `f001c8a7943425e12cf4d0498a82837acf722eb6`.
They apply to the filtered tree at the repository root, retaining upstream
C/header filenames and regression-test paths. Related changes may be grouped
below a single feature merge. Dependencies remain explicit; the SCRAM verifier
change is independent of the routing changes.

Each aggregate has the same shared patch and feature-group parents, plus
its major's pristine upstream tip. The aggregates resolve build, SQL and export
lists, renamed regression files and version-specific APIs. PostgreSQL 19
adaptations belong in `fdw_base_19-`; shared behavior belongs in the common
patches. Keep `fdw_patch_base` fixed when updating one major so its upstream
branch's changes do not enter the other major.

Each `fdw_base_MAJOR` bookmarks a pure directory-move change with **one parent**:
its aggregate. It puts the aggregate's exact tree under `pgwrh_fdw/MAJOR/`.
Both move changes are parents of the project integration ancestor of `main`.
The project branch contains the common wrapper, documentation and tools; the
versioned FDW sources come exclusively from the move parents.

```mermaid
graph TD
    B[fdw_patch_base] --> U18[Upstream 18]
    B --> P[Shared patches and feature groups]
    B --> U19[Upstream 19]
    U18 --> A18[Aggregate 18]
    P --> A18
    P --> A19[Aggregate 19]
    U19 --> A19
    A18 --> M18[Directory move: fdw_base_18]
    A19 --> M19[Directory move: fdw_base_19]
    M18 --> MAIN[main]
    M19 --> MAIN
    PROJECT[Project changes] --> MAIN
```

There is one `main` and one pgwrh release version. SQL, UI and `pgwrh_wait`
remain shared; the wait extension isolates PostgreSQL API differences in
`pgwrh_wait/src/compat.h`. The common `pgwrh_fdw/Makefile` selects its versioned
directory using `PG_CONFIG` and excludes the test-only `context_probe` module
from installed packages. Existing releases and historical subtree imports remain
in ancestry; current maintenance uses jj directory moves and merges.

```sh
# Shared functional changes, aggregate adaptations, and the mechanical move:
jj log -r 'parents(fdw_base_18-) & parents(fdw_base_19-)'
jj diff -r fdw_base_19-
jj diff -r fdw_base_19 --summary
```

Functional patches can be reviewed or exported without removing a directory
prefix. Proposals for PostgreSQL itself still need to select relevant changes
and adapt the extension-specific API and tests.

The lookup implementation is shared in `sqmzztxu` and `yznpkzqo`; the related
LIMIT regression adaptation is `ynssotmk`. These are additional parents of the
existing aggregates (`lvwzssxp` for 18 and `mqpxytlz` for 19), whose existing
directory moves have been refreshed. No additional aggregate was created.
The shared follow-up `ysvzmywq` removes the lookup type allowlists, reuses FDW
shippability and proves partition routing separately; it is another parent of
those same aggregates.

## Partial aggregation feature group

Issue #7 is grouped under `mymyvlul` (`codex/partial-aggregate-pushdown`).
It contains `tvtzuoto` (planner and safe scalar-state deparsing), `zuvyxuxp`
(regressions, managed-shard test, documentation and transfer demo), and their
follow-up `pyxuykkk` (smallint/integer average states and coverage). The first two
start at `fdw_patch_base`. The regression-plan update `ylzxomyk` depends on the
shared FDW identity patch so it updates the renamed expected-output file and
disappears cleanly when the feature is removed. All four are parents of the
feature group. Only the feature group is an additional direct parent of
`lvwzssxp` and `mqpxytlz`; none of its components is also a direct parent.
The existing aggregates and directory-move changes are retained.

See [supported signatures and the reproducible measurement](18/PARTIAL_AGGREGATES.md)
([PostgreSQL 19 copy](19/PARTIAL_AGGREGATES.md)). The project test runners discover
feature-owned `test_feature_*.py` and `managed_test_*.py` files from the selected FDW tree,
so removing the feature also removes its tests without leaving a broken test
command. To remove it from an unpublished aggregate and refresh its move:

```sh
jj rebase -s lvwzssxp -o 'parents(lvwzssxp) ~ mymyvlul'
python3 pgwrh_fdw/tools/refresh-layout.py 18
jj rebase -s mqpxytlz -o 'parents(mqpxytlz) ~ mymyvlul'
python3 pgwrh_fdw/tools/refresh-layout.py 19
```

For already-published history, follow the duplicate-aggregate workflow below.

## Pipelining feature group

`lqyysrzn` (`codex/fdw-pipelining`) groups the implementation for both versions:

| Change | Purpose | Parent |
| --- | --- | --- |
| `vvxupymt` | Connection-owned operation model and AGPL notice | `fdw_patch_base` |
| `ynqntoyx` | Bounded, nonblocking libpq transport and depth option | `vvxupymt` |
| `lxotrnym` | Transaction barriers, cancellation, rollback and result lifetime | `ynqntoyx` |
| `ruunxnzr` | Pipelined cursor creation and async FETCH scheduling | `lxotrnym` |
| `lyysrxol` | pgwrh namespace mappings | FDW identity change `sqylzvly` |
| `uyymlztr` | Feature-owned tests, documentation and benchmark | `fdw_patch_base` |

The feature group's parents are `ruunxnzr`, `lyysrxol` and `uyymlztr`. Only the
group is an additional direct parent of the existing `lvwzssxp` (18) and
`mqpxytlz` (19) aggregates. Runtime changes 1–4 do not depend on pgwrh routing,
transaction-context propagation, lookup joins or other pgwrh features. The
existing `myrzuoso`/`pozuquuz` directory moves remain directly above their
respective aggregates. PostgreSQL 19's result wrappers and changed error API
are adapted in its aggregate; PG18 and PG19 regression expectations stay separate.

All new code is AGPL-3.0-only. An upstream submission would require a separate,
explicit relicensing decision. This group implements cursor pipelining only.

To remove the unpublished feature, drop `codex/fdw-pipelining` from an
aggregate's direct parents, resolve its integration conflicts, and refresh its
pure directory move. The removal check found conflicts in `Makefile` and
`option.c` for both versions, and additionally `pipeline.c` and `postgres_fdw.c`
for PG19: remove the pipeline-specific compatibility code along with the feature.
The feature's tests and benchmark then disappear with its source files. Do not
remove existing parents or copy one major's source over the other. Disposable
removal builds were checked against both pre-feature trees and their async tests.

## Cursor-free streaming feature group

`tkrwonls` (`codex/fdw-cursor-free-streaming`) builds on pipelining:

| Change | Purpose | Parents |
| --- | --- | --- |
| `qxyrnpyo` | Chunked SELECT operations and spillable raw-result storage | `ruunxnzr` |
| `zslvuyns` | Streaming option, scan lifecycle and async execution | `qxyrnpyo` |
| `qsrmlkvz` | Isolated pgwrh names for streaming helpers | `lyysrxol` |
| `vutoskvv` | Virtual options, lookup parameters and lookup rescans | `zslvuyns`, `qsrmlkvz`, `sqmzztxu`, `rvzxtrto` |
| `zowutmnp` | Feature-owned tests, benchmark, usage and attribution | `uyymlztr` |

The group's parents are `vutoskvv`, `zowutmnp` and `lqyysrzn`. Only the group
is added to the existing version aggregates. The two generic runtime changes
have no dependency on pgwrh routing or lookup joins. PG19 result wrappers,
TupleDesc finalization and error API differences remain in its aggregate.
All additions remain AGPL-3.0-only; upstream provenance is in
[STREAMING.md](18/STREAMING.md).

To remove streaming, drop its group from an aggregate's parents and resolve
integration conflicts, retaining the pipeline implementation. Disposable checks
restored both pre-streaming trees byte for byte; the integration files were
`Makefile`, `lookup_join.c`, `postgres_fdw.c`, and PG19's `pipeline.c`.
To remove pipelining too, drop **both groups**, remove its build object and
resolve the version-specific pipeline adaptations described above. Both
pre-pipelining trees were restored byte for byte and their ordinary and
shared-connection async scans passed. Refresh the existing directory moves
after either operation. Removing only pipelining's direct parent while keeping
streaming cannot remove pipelining: it remains an explicit dependency.

## Changing shared behavior

Edit the relevant unpublished shared change once. jj rebases both descendant
aggregates; resolve their compatibility conflicts and test both major versions.
Upstream regression files retain their major-specific contents even though the
identity patch renames them. Do not copy one major's tests over the other.

With jj 0.45.1, a rebased directory move can leave edits at the old root path and
outdated copies under the destination. After resolving aggregate conflicts,
refresh the mechanical moves from the pgwrh workspace:

```sh
python3 pgwrh_fdw/tools/refresh-layout.py 18
python3 pgwrh_fdw/tools/refresh-layout.py 19
```

The helper builds an exact move in a temporary jj workspace and restores that
tree into the existing move change. Its aggregate remains its sole parent.
Descendant rebases carry the refreshed directories into `main`. Your current
working change and unrelated edits are preserved. The helper verifies source
tree identity, including file modes and symlinks, and accepts `--repository PATH`
when invoked from another workspace. It never publishes or pushes anything.

Check either move without refreshing it:

```sh
python3 pgwrh_fdw/tools/refresh-layout.py 18 --check
python3 pgwrh_fdw/tools/refresh-layout.py 19 --check
```

For published patches, add shared follow-up changes instead of rewriting
released revisions. Published aggregates can be duplicated individually onto
their existing parents plus the new shared change; the functional patch graph
itself is not copied. Create a new directory move with:

```sh
python3 pgwrh_fdw/tools/refresh-layout.py 19 --aggregate NEW_AGGREGATE_ID
```

For an unpublished integration, replace its previous move parent while retaining
all other parents (record `OLD_MOVE_ID` before creating the replacement):

```sh
jj rebase -s main -o 'parents(main) ~ OLD_MOVE_ID' -o fdw_base_19
```

To preserve a published `main`, create a project-only child first, then merge the
move bookmarks. Removing the old directory copies from that project parent
prevents future edits from conflicting with stale snapshots:

```sh
jj new main -m 'Prepare project files for refreshed FDWs'
jj restore --from 'root()' pgwrh_fdw/18 pgwrh_fdw/19
jj new @ fdw_base_18 fdw_base_19 -m 'Integrate refreshed FDWs'
```

Review the integration before advancing `main`. Shared standalone ancestry may
produce initial conflicts at root paths such as `Makefile` and `.gitignore`;
retain the project versions there and the FDW versions inside their directories.
Do not merge a root-level aggregate directly into `main`.

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
The aggregates, directory moves, `main` and working files remain untouched.

Keep `git filter-branch --subdirectory-filter contrib/postgres_fdw` as the
extraction method so filtered history retains compatible ancestry. The first
import of a major creates its pristine bookmark; later imports must be
fast-forwards. Replace only the unpublished aggregate's upstream parent:

```sh
jj rebase -s fdw_base_19- \
  -o 'parents(fdw_base_19-) ~ OLD_UPSTREAM_ID' \
  -o upstream/postgres_fdw_19
```

For an already published aggregate, duplicate only that aggregate instead:

```sh
jj duplicate fdw_base_19- \
  -o 'parents(fdw_base_19-) ~ OLD_UPSTREAM_ID' \
  -o upstream/postgres_fdw_19
```

Resolve compatibility conflicts in the aggregate, retaining the shared patch
parents. Then refresh its existing directory move or create a new one with
`--aggregate NEW_AGGREGATE_ID`, as appropriate. The other major's aggregate and
directory stay unchanged. Review upstream SQL and tests alongside C changes;
refresh this provenance table, `COPYRIGHT`, and matching CI and Nix source pins.

## Validation and release

Select a matching `PG_CONFIG` for each major and run:

```sh
python3 test/pgwrh_fdw/test_upstream_import.py
python3 test/pgwrh_fdw/test_jj_layout.py
make test-fdw
PG_SOURCE=/path/to/matching/postgres/source make test-fdw-tap
make test-packaging
make test-wait
make test-pgwrh
make test-ui
```

The layout tests need jj and Git; both are included in `nix develop .#tests-18`
and `.#tests-19`. They exercise shared edits, additions, deletions, per-major
upstream updates, preservation of working edits and exact directory contents.
The importer tests cover dirty working files, immutable provenance tags,
unrelated-history rejection and preservation of the other major.

Publish `fdw_patch_base`, the pristine and directory-move bookmarks, and annotated
upstream tags together with the reviewed integration. See [PostgreSQL
compatibility](../docs/development/postgres-versions.md) for the preview checklist.

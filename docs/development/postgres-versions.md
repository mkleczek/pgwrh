# PostgreSQL versions and the 19 preview

pgwrh has one main branch and one extension version across PostgreSQL majors.
Only `pgwrh_fdw` has a separate upstream and patch graph per major. Their exact
patched trees are imported into `pgwrh_fdw/18/` and `pgwrh_fdw/19/`; `PG_CONFIG`
selects the matching implementation. SQL, UI and the wait extension stay shared.
See the [FDW workflow](../../pgwrh_fdw/UPSTREAM.md) for updating either graph.

| Target | FDW source | Status |
| --- | --- | --- |
| PostgreSQL 18 | 18.3, unchanged | Default build |
| PostgreSQL 19 | 19 Beta 3 | Development preview |

Both use pg_background 2.0.3. Nix pins that exact upstream release; native
packages require at least that version. This dependency is not forked in-tree.

The published pgwrh 1.0.0-alpha1 tag and artifacts remain unchanged. Development
sources keep their existing extension version until the next pgwrh release is
chosen; do not overwrite the published alpha1 artifacts with this checkout.

## Checking a major

```sh
nix develop .#tests-19 --command bash test/run-functional.sh
nix develop .#tests-19 --command make test-fdw test-packaging
nix build .#postgresql-19
```

Use `18` for the other major. Both run the same core, logical-replication wait
and real PostgREST UI tests. Each FDW also runs its own retained upstream tests,
SCRAM TAP tests and symbol isolation check. Native binaries are rebuilt for each
server major; one major's library cannot be reused with another.

The release matrix builds DEBs and container images for both majors, plus Nix
bundles on Linux and macOS. PostgreSQL 19 RPM metadata is prepared, but that matrix
entry is excluded until PGDG publishes `pg_background_19 >= 2.0.3`. PostgreSQL 18
RPM builds remain required. This is an external packaging gap, not permission to
skip the PostgreSQL 19 functional suites.

## Before releasing against PostgreSQL 19 final

1. Import the selected PostgreSQL 19 release and port the 13-patch graph from the
   previous pristine base. Verify the aggregate, then subtree-merge it into
   `pgwrh_fdw/19`.
2. Update the source pin/hash in `nix/postgresql-19.nix`, the FDW CI source pin,
   container base tag and this provenance documentation. Keep PostgreSQL 18's
   graph and directory intact.
3. Remove beta-specific APT/RPM repository settings once PGDG moves 19 into its
   ordinary repositories. Enable the PostgreSQL 19 RPM matrix entry when its
   pg_background package is available.
4. Run both major-version suites and all artifact checks from the exact release
   source archive. Choose the next pgwrh version and prepare a fresh tag; do not
   retag alpha1. Follow [the release procedure](../releasing.md).

These suites exercise clusters whose members use the same PostgreSQL major.
Cross-major controller/replica combinations and rolling PostgreSQL major upgrades
are not covered by this preparation.

## Local validation, 2026-09-21

The preparation was checked with PostgreSQL 18.6 and 19 Beta 3 on macOS arm64,
using pg_background 2.0.3 for both:

- Each major: 131 core/UI tests, including the real PostgREST HTTP tests, and
  60 wait tests passed with no skips.
- Each FDW: 104 context/routing tests, both upstream SQL regressions, the
  isolation regression and prescribed-symbol export checks passed.
- PostgreSQL 19: all 13 individual FDW patches compiled; all 9 SCRAM TAP tests
  passed against matching upstream source. Its FDW suite also passed on Linux
  arm64, including ELF export checks.
- Installed Nix bundles, staged install/uninstall modes and the PostgreSQL 19
  Ubuntu 24.04 DEB and Debian container installation checks passed.
- The PostgreSQL 19 Compose demo passed initial and repeated setup, returned
  100 rows from each replica and served the read-only console.
- Release metadata, workflow lint, signed repository generation for both DEB
  names, and synthetic upstream/subtree update tests passed. The latter verify
  that updating one imported directory preserves the other exactly.

The expanded CI matrix remains responsible for the other operating-system and
architecture combinations. No release tag or published artifact was changed.

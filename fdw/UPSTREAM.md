# Upstream provenance

Source: https://github.com/postgres/postgres

Initial release: PostgreSQL 18.3, tag `REL_18_3`.
Commit: `62d6c7d3df6287f1bd83199c1a746e50d31571a0`.

The source files in `fdw/` fork `contrib/postgres_fdw`.
The `upstream/postgres_fdw` branch contains the unmodified, filtered history
of only that directory (569 commits at the initial import). Its initial tip is
`6ba739cca5fb00282732191c46a61d3408836284`. The annotated `upstream/REL_18_3`
tag records the original release tag and full-repository commit. The
extraction avoids importing PostgreSQL's server source into this project.
`fdw/COPYRIGHT` is PostgreSQL's license, copied from the same release.

The initial subtree import preserves the former pgwrh_fdw release `v0.1.0`,
commit `59cd1a0be8fcfb8fd5b3633ca0158a35b3a72c9d`. Its namespace, behavior, and
release commits remain in history.
The standalone repository is no longer needed to build or update this component.

Import or update from an existing local PostgreSQL Git checkout, running from
the pgwrh root:

```sh
python3 fdw/tools/import-upstream.py /path/to/postgres REL_18_N
```

The tool uses `git filter-branch --subdirectory-filter contrib/postgres_fdw`
in a disposable clone of the source repository. It fetches only the resulting
history into the upstream branch in pgwrh. Use a later, explicitly selected
PostgreSQL 18 release in place of `REL_18_N`; retain the extraction method so
the filtered history has compatible ancestry. Then, from the pgwrh root, run:

```sh
git subtree merge --prefix=fdw upstream/postgres_fdw
```

Use subtree merges without `--squash`; an ordinary merge would target the wrong
directory. Run with a clean working tree. Resolve update conflicts, review
upstream API changes, update this provenance file and `fdw/COPYRIGHT`, and run
`make test-fdw`, `make test-packaging`, and the native wait tests before releasing.
Commit maintenance adaptations separately from the upstream merge. Never rewrite
an existing upstream snapshot. On a fresh clone, the import script can recreate
the local upstream branch; the imported baseline is also reachable through the
subtree history. Preserve the upstream branch and provenance tags when publishing.

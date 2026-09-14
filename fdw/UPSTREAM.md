# Upstream provenance

Source: https://github.com/postgres/postgres

Initial release: PostgreSQL 18.3, tag `REL_18_3`.
Commit: `62d6c7d3df6287f1bd83199c1a746e50d31571a0`.

The source files at the repository root fork `contrib/postgres_fdw`.
The `upstream/postgres_fdw` branch contains the unmodified, filtered history
of only that directory (569 commits at the initial import). Its initial tip is
`6ba739cca5fb00282732191c46a61d3408836284`. The annotated `upstream/REL_18_3`
tag records the original release tag and full-repository commit. The
extraction avoids importing PostgreSQL's server source into this project.
The root COPYRIGHT is PostgreSQL's license, copied from the same release.

Import or update from an existing local PostgreSQL Git checkout:

```sh
python3 tools/import-upstream.py /path/to/postgres REL_18_3
```

The tool uses `git filter-branch --subdirectory-filter contrib/postgres_fdw`
in a disposable clone of the source repository. It fetches only the resulting
history into the upstream branch. Then run `git merge upstream/postgres_fdw`.
Run with a clean working tree. Resolve update conflicts, review upstream API changes,
update this provenance file and COPYRIGHT, then run all tests before committing
any follow-up adaptations. Never rewrite an existing upstream snapshot.

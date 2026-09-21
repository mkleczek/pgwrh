#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Import pristine postgres_fdw history without changing patches or main."""
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def git(repo, *args):
    return subprocess.check_output(
        ["git", "-C", str(repo), *args], text=True
    ).strip()


def main():
    if len(sys.argv) != 3 or not re.fullmatch(r"REL_(18|19)_(\d+|BETA\d+|RC\d+)", sys.argv[2]):
        sys.exit("usage: python3 pgwrh_fdw/tools/import-upstream.py /local/postgres/repo REL_MAJOR_RELEASE")
    source = Path(sys.argv[1]).resolve(strict=True)
    tag = sys.argv[2]
    commit = git(source, "rev-parse", tag + "^{commit}")
    major = tag.split("_")[1]
    upstream = "refs/heads/upstream/postgres_fdw" + ("" if major == "18" else "_" + major)
    exists = subprocess.run(["git", "-C", str(ROOT), "show-ref", "--verify", "--quiet", upstream]).returncode == 0
    previous = git(ROOT, "rev-parse", upstream) if exists else None
    tag_ref = "refs/tags/upstream/" + tag
    if subprocess.run(["git", "-C", str(ROOT), "show-ref", "--verify", "--quiet", tag_ref]).returncode == 0:
        sys.exit(f"{tag_ref} already exists; upstream snapshots are immutable")

    with tempfile.TemporaryDirectory(prefix="pgwrh-fdw-import-") as directory:
        filtered = Path(directory) / "postgres"
        git(ROOT, "clone", "--shared", "--no-checkout", str(source), str(filtered))
        git(filtered, "sparse-checkout", "init", "--cone")
        git(filtered, "sparse-checkout", "set", "contrib/postgres_fdw")
        git(filtered, "checkout", "-b", "fdw-import", commit)
        subprocess.run(
            ["git", "filter-branch", "--subdirectory-filter", "contrib/postgres_fdw", "--", "fdw-import"],
            cwd=filtered, env=dict(os.environ, FILTER_BRANCH_SQUELCH_WARNING="1"),
            check=True, stdout=subprocess.DEVNULL,
        )
        snapshot = git(filtered, "rev-parse", "fdw-import")

        if previous:
            subprocess.run(["git", "-C", str(filtered), "merge-base", "--is-ancestor", previous, snapshot], check=True)
        for key in ("user.name", "user.email"):
            git(filtered, "config", key, git(ROOT, "config", "--get", key))
        git(filtered, "tag", "-a", "upstream/" + tag, snapshot, "-m",
            f"PostgreSQL {tag}\nUpstream commit: {commit}\nFiltered path: contrib/postgres_fdw")
        # No force: a failed or conflicting import leaves both refs unchanged.
        # The patched aggregates move only after review.
        git(ROOT, "fetch", "--atomic", "--no-tags", str(filtered),
            "refs/heads/fdw-import:" + upstream, tag_ref + ":" + tag_ref)

    print(f"Imported {tag} ({commit}); filtered tip {snapshot}")
    print(f"Updated {upstream}; patched aggregates, main and working files are unchanged.")
    print("See pgwrh_fdw/UPSTREAM.md for review, provenance and test requirements.")


if __name__ == "__main__":
    main()

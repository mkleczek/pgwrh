#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Extract ONLY contrib/postgres_fdw history in a disposable local clone."""
import re
import subprocess
import sys
import os
import tempfile
from pathlib import Path


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


if len(sys.argv) != 3 or not re.fullmatch(r"REL_18_\d+", sys.argv[2]):
    sys.exit("usage: tools/import-upstream.py /local/postgres/repo REL_18_N")
source = str(Path(sys.argv[1]).resolve(strict=True))
tag = sys.argv[2]
if git("status", "--porcelain"):
    sys.exit("Commit or stash existing work first.")
commit = git("-C", source, "rev-parse", tag + "^{commit}")
with tempfile.TemporaryDirectory(prefix="pgwrh-fdw-upstream-") as directory:
    clone = str(Path(directory) / "postgres")
    git("clone", "--shared", "--no-checkout", source, clone)
    git("-C", clone, "sparse-checkout", "init", "--cone")
    git("-C", clone, "sparse-checkout", "set", "contrib/postgres_fdw")
    git("-C", clone, "checkout", "-b", "fdw-import", tag)
    subprocess.run(["git", "filter-branch", "--subdirectory-filter",
                    "contrib/postgres_fdw", "--", "fdw-import"], cwd=clone,
                   env=dict(os.environ, FILTER_BRANCH_SQUELCH_WARNING="1"),
                   check=True, stdout=subprocess.DEVNULL)
    snapshot = git("-C", clone, "rev-parse", "fdw-import")
    # No force: the new filtered history must extend the existing branch.
    git("fetch", "--no-tags", clone,
        "fdw-import:refs/heads/upstream/postgres_fdw")
git("tag", "-a", "upstream/" + tag, snapshot, "-m",
    f"PostgreSQL {tag}\nUpstream commit: {commit}\n"
    "Filtered path: contrib/postgres_fdw")
print(f"Imported {tag} ({commit}); filtered tip {snapshot}")
print("Next: git merge upstream/postgres_fdw")
print("Update UPSTREAM.md and COPYRIGHT, review lifecycle changes, and test.")

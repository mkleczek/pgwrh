#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Run upstream SCRAM TAP with matching source modules and staged extension."""
import os
from pathlib import Path
import subprocess
import sys

from support import BIN, Cluster, ROOT, config

source = os.environ.get("PG_SOURCE")
if not source or not (Path(source) / "src/test/perl/PostgreSQL/Test/Cluster.pm").exists():
    sys.exit("Set PG_SOURCE to matching PostgreSQL 18 source (for src/test/perl).")
cluster = Cluster()
try:
    cluster.setup()
    regress = Path(config("--pgxs")).parents[1] / "test/regress/pg_regress"
    env = dict(os.environ,
               PATH=str(BIN) + os.pathsep + os.environ["PATH"],
               PERL5LIB=str(Path(source) / "src/test/perl") + os.pathsep + os.environ.get("PERL5LIB", ""),
               PG_REGRESS=str(regress),
               PGWRH_FDW_TEST_STAGE=str(cluster.stage),
               TESTDIR=str(ROOT),
               PG_TEST_PORT_DIR=str(cluster.path))
    # Respect a selected Perl runtime, e.g. a development environment's Perl.
    subprocess.run([os.environ.get("PERL", "perl"), "t/001_auth_scram.pl"],
                   cwd=ROOT, env=env, check=True)
finally:
    cluster.close()

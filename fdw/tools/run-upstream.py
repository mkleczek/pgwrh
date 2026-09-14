#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Run retained SQL/isolation regressions on a private, staged cluster."""
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test"))
from support import Cluster, PG_CONFIG, ROOT

cluster = Cluster()
try:
    cluster.setup()
    env = dict(os.environ, PGHOST=str(cluster.path), PGPORT=str(cluster.port))
    subprocess.run(["make", "installcheck", "PG_CONFIG=" + PG_CONFIG,
                    "TAP_TESTS=", *sys.argv[1:]], cwd=ROOT, env=env, check=True)
finally:
    cluster.close()

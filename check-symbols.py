#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Check the complete dynamic export surface, including on ELF/Linux."""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent
library = root / ("pgwrh_fdw.dylib" if sys.platform == "darwin" else "pgwrh_fdw.so")
args = ["nm", "-gU"] if sys.platform == "darwin" else ["nm", "-D", "--defined-only"]
output = subprocess.check_output([*args, str(library)], text=True)
actual = {line.split()[-1] for line in output.splitlines() if line.split()}
if sys.platform == "darwin":
    actual = {name[1:] for name in actual}
functions = {"pgwrh_fdw_" + name for name in (
    "handler", "validator", "get_connections", "get_connections_1_2",
    "disconnect", "disconnect_all", "set_members")}
expected = {"Pg_magic_func", "_PG_init"} | functions | {"pg_finfo_" + f for f in functions}
if actual != expected:
    sys.exit(f"Unexpected exports: {actual - expected}; missing: {expected - actual}")
print(f"PASS: {len(actual)} prescribed exports; all internal helpers hidden")

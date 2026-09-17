#!/usr/bin/env python3
"""Check the complete staged package and uninstall without touching PostgreSQL."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
pg_config = shutil.which(os.environ.get("PG_CONFIG", "pg_config"))
if pg_config is None:
    sys.exit("Set PG_CONFIG to a PostgreSQL 18 pg_config executable")
PG_CONFIG = str(Path(pg_config).resolve())


def config(option):
    return subprocess.check_output([PG_CONFIG, option], text=True).strip()


def make(log, *args):
    command = ["make", "-j4", "PG_CONFIG=" + PG_CONFIG, *args]
    with log.open("a") as output:
        result = subprocess.run(command, cwd=ROOT, stdout=output,
                                stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f"{' '.join(command)} failed:\n{log.read_text()}")


def files(directory):
    return {p.relative_to(directory) for p in directory.rglob("*")
            if p.is_file() or p.is_symlink()}


def check_install(stage, log, extensions, options):
    make(log, "install", "DESTDIR=" + str(stage), *options)
    sharedir = stage / config("--sharedir").lstrip("/") / "extension"
    libdir = stage / config("--pkglibdir").lstrip("/")
    actual = {p.stem for p in sharedir.glob("*.control")}
    assert actual == set(extensions), (actual, extensions)

    sql = set()
    for extension in extensions:
        control = (sharedir / (extension + ".control")).read_text()
        version = re.search(r"^default_version\s*=\s*'([^']+)'", control, re.M)[1]
        assert version == "1.0.0", (extension, version)
        sql.add(f"{extension}--{version}.sql")
    assert {p.name for p in sharedir.glob("*.sql")} == sql

    support = sharedir.parent / "pgwrh_ui"
    if "pgwrh_ui" in extensions:
        sources = ["README.md", "readonly.sql", "operator.sql", "postgrest.conf",
                   "vendor/HTMX-LICENSE"]
        assert {p.name for p in files(support)} == {Path(p).name for p in sources}
        for source in sources:
            assert (support / Path(source).name).read_bytes() == (ROOT / "pgwrh_ui" / source).read_bytes()
    else:
        assert not files(support), "UI support files installed without pgwrh_ui"

    libraries = {p.name for p in libdir.glob("*") if p.suffix in (".so", ".dylib")}
    expected = set(extensions) - {"pgwrh", "pgwrh_ui"}
    assert {Path(p).stem for p in libraries} == expected, libraries

    # Uninstall must remove exactly the package payload, including LLVM files,
    # and leave unrelated files alone.
    sentinel = stage / "unrelated file"
    sentinel.write_text("keep\n")
    make(log, "uninstall", "DESTDIR=" + str(stage), *options)
    assert files(stage) == {Path("unrelated file")}, files(stage)
    assert sentinel.read_text() == "keep\n"
    print(f"PASS: {stage.name} install and uninstall", flush=True)


def main():
    with tempfile.TemporaryDirectory(prefix="pgwrh-package-") as temporary:
        directory = Path(temporary)
        log = directory / "build.log"
        make(log, "clean")
        # PGXS's install_llvm_module splits DESTDIR on whitespace when it
        # creates nested bitcode directories. Exercise LLVM with normal RPM
        # staging paths, and test DESTDIR quoting separately without bitcode.
        check_install(directory / "combined-stage", log,
                      ["pgwrh", "pgwrh_ui", "pgwrh_fdw", "pgwrh_wait"], [])
        check_install(directory / "wait-stage", log, ["pgwrh", "pgwrh_ui", "pgwrh_wait"],
                      ["WITH_FDW=0"])
        check_install(directory / "fdw-stage", log, ["pgwrh", "pgwrh_ui", "pgwrh_fdw"],
                      ["WITH_LSN_WAIT=0"])
        check_install(directory / "sql pgxs stage", log, ["pgwrh", "pgwrh_ui"],
                      ["WITH_LSN_WAIT=0", "WITH_FDW=0"])
        check_install(directory / "sql no-pgxs stage", log, ["pgwrh", "pgwrh_ui"],
                      ["NO_PGXS=1"])
        for extension in ("pgwrh", "pgwrh_ui", "pgwrh_wait", "pgwrh_fdw"):
            check_install(directory / (extension + "-standalone-stage"), log,
                          [extension], ["-C", str(ROOT / extension)])
        check_install(directory / "combined space stage", log,
                      ["pgwrh", "pgwrh_ui", "pgwrh_fdw", "pgwrh_wait"], ["with_llvm=no"])
    print("PASS: all packaging modes (no system installation)", flush=True)


if __name__ == "__main__":
    main()

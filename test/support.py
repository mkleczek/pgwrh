# SPDX-License-Identifier: AGPL-3.0-only
"""Dependency-free libpq client and temporary PostgreSQL 18 test cluster."""
import ctypes as C
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PG_CONFIG = os.environ.get("PG_CONFIG", "pg_config")


def config(option):
    return subprocess.check_output([PG_CONFIG, option], text=True).strip()


BIN = Path(config("--bindir"))
libdir = Path(config("--libdir"))
libpq = next(p for suffix in ("dylib", "so")
             for p in [libdir / ("libpq." + suffix)] if p.exists())
PQ = C.CDLL(str(libpq))
for name, result, args in [
    ("PQconnectdb", C.c_void_p, [C.c_char_p]),
    ("PQstatus", C.c_int, [C.c_void_p]),
    ("PQerrorMessage", C.c_char_p, [C.c_void_p]),
    ("PQfinish", None, [C.c_void_p]),
    ("PQexec", C.c_void_p, [C.c_void_p, C.c_char_p]),
    ("PQresultStatus", C.c_int, [C.c_void_p]),
    ("PQresultErrorMessage", C.c_char_p, [C.c_void_p]),
    ("PQresultErrorField", C.c_char_p, [C.c_void_p, C.c_int]),
    ("PQntuples", C.c_int, [C.c_void_p]),
    ("PQnfields", C.c_int, [C.c_void_p]),
    ("PQgetvalue", C.c_char_p, [C.c_void_p, C.c_int, C.c_int]),
    ("PQgetisnull", C.c_int, [C.c_void_p, C.c_int, C.c_int]),
    ("PQclear", None, [C.c_void_p]),
]:
    fn = getattr(PQ, name)
    fn.restype, fn.argtypes = result, args


def literal(value):
    return "E'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


class PgError(Exception):
    def __init__(self, message, sqlstate):
        super().__init__(message)
        self.sqlstate = sqlstate


class Connection:
    def __init__(self, conninfo):
        self.ptr = PQ.PQconnectdb(conninfo.encode())
        if PQ.PQstatus(self.ptr) != 0:
            message = PQ.PQerrorMessage(self.ptr).decode()
            self.close()
            raise RuntimeError(message)

    def sql(self, query):
        result = PQ.PQexec(self.ptr, query.encode())
        if not result:
            raise RuntimeError(PQ.PQerrorMessage(self.ptr).decode())
        try:
            if PQ.PQresultStatus(result) not in (1, 2):
                state = PQ.PQresultErrorField(result, ord('C'))
                raise PgError(PQ.PQresultErrorMessage(result).decode(),
                              state.decode() if state else None)
            return [tuple(None if PQ.PQgetisnull(result, r, c)
                          else PQ.PQgetvalue(result, r, c).decode()
                          for c in range(PQ.PQnfields(result)))
                    for r in range(PQ.PQntuples(result))]
        finally:
            PQ.PQclear(result)

    def scalar(self, query):
        return self.sql(query)[0][0]

    def close(self):
        if self.ptr:
            PQ.PQfinish(self.ptr)
            self.ptr = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Cluster:
    def __init__(self):
        self.path = Path(tempfile.mkdtemp(prefix="pgwrh-fdw-"))
        self.data = self.path / "data"
        self.log = self.path / "postgres.log"
        self.stage = ROOT / ".build" / "stage"
        self.started = False
        # TCP is disabled, but a distinct port also names the private socket.
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]

    def setup(self):
        subprocess.run(["make", "-s", "-j4", "PG_CONFIG=" + PG_CONFIG],
                       cwd=ROOT, check=True)
        subprocess.run(["make", "-s", "-C", "test", "PG_CONFIG=" + PG_CONFIG],
                       cwd=ROOT, check=True)
        (self.stage / "extension").mkdir(parents=True, exist_ok=True)
        for p in ROOT.glob("pgwrh_fdw--*.sql"):
            shutil.copy(p, self.stage / "extension" / p.name)
        control = (ROOT / "pgwrh_fdw.control").read_text()
        control = control.replace("$libdir/pgwrh_fdw", "pgwrh_fdw")
        (self.stage / "extension/pgwrh_fdw.control").write_text(control)
        for stem, folder in [("pgwrh_fdw", ROOT), ("context_probe", ROOT / "test")]:
            library = next(p for p in folder.glob(stem + ".*")
                           if p.suffix in (".so", ".dylib"))
            shutil.copy(library, self.stage / library.name)
        subprocess.run([str(BIN / "initdb"), "-D", str(self.data), "-A", "trust",
                        "--no-locale", "--encoding=UTF8"],
                       check=True, stdout=subprocess.DEVNULL)
        with (self.data / "postgresql.conf").open("a") as f:
            f.write(f"\nlisten_addresses = ''\nport = {self.port}\n"
                    f"unix_socket_directories = '{self.path}'\n"
                    f"dynamic_library_path = '{self.stage}:$libdir'\n"
                    f"extension_control_path = '{self.stage}:$system'\n"
                    "log_statement = 'all'\nlog_line_prefix = '%p '\n"
                    "max_prepared_transactions = 10\n")
        subprocess.run([str(BIN / "pg_ctl"), "-D", str(self.data), "-l",
                        str(self.log), "-w", "start"],
                       check=True, stdout=subprocess.DEVNULL,
                       env=dict(os.environ, PGHOST=str(self.path)))
        self.started = True
        print("PostgreSQL:", config("--version"), "test logs:", self.log, flush=True)
        return self

    def connect(self, database="postgres", user=None):
        info = f"host={self.path} port={self.port} dbname={database}"
        if user:
            info += " user=" + user
        return Connection(info)

    def close(self):
        if self.started:
            subprocess.run([str(BIN / "pg_ctl"), "-D", str(self.data), "-m",
                            "immediate", "-w", "stop"], check=True,
                           stdout=subprocess.DEVNULL)
        # Keep logs and databases for failure investigation; temporary paths
        # are private and never touch an existing PostgreSQL installation.

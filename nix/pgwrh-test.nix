{
  coreutils,
  findutils,
  gitMinimal,
  gnumake,
  gnused,
  lib,
  postgresql,
  pythonEnv,
  shellcheck,
  writers,
}:

let
  checkedShellScript =
    name: script:
    writers.writeBashBin name
      {
        check = "${lib.getExe shellcheck.unwrapped} -x";
        makeWrapperArgs = [
          "--prefix"
          "PATH"
          ":"
          (lib.makeBinPath [
            coreutils
            findutils
            gitMinimal
            gnumake
            gnused
            postgresql
            pythonEnv
          ])
        ];
      }
      script;
in
checkedShellScript "pgwrh-test" ''
  set -euo pipefail

  if repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    :
  elif [[ -n "''${PGWRH_REPO_ROOT:-}" ]]; then
    repo_root="$PGWRH_REPO_ROOT"
  else
    echo "error: could not determine pgwrh repository root" >&2
    exit 1
  fi

  cd "$repo_root"

  export PG_BIN="${postgresql}/bin"
  export PG_CONFIG="${postgresql}/bin/pg_config"
  export PGWRH_TEST_BIN_DIR="${postgresql}/bin"
  export PGWRH_TEST_EXT_PATHS="$repo_root/.build/testgres-ext"

  make testgres-ext
  exec pytest "$@"
''

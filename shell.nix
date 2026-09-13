with import <nixpkgs> {};

let
  xpg = callPackage ./nix/xpg.nix { inherit fetchFromGitHub; };
  testgres = callPackage ./nix/testgres.nix {};

  postgresql = postgresql_18.withPackages (ps: [
    ps.pg_background
  ]);

  pythonEnv = python3.withPackages (ps: [
    ps.pytest
    testgres
  ]);

  pgwrhTest = callPackage ./nix/pgwrh-test.nix {
    inherit postgresql pythonEnv;
  };
in
mkShell {
  buildInputs = [
    pgwrhTest
    xpg.xpg
    pythonEnv
    postgresql
    gnumake
  ];

  shellHook = ''
    export HISTFILE=.history
    export PGWRH_REPO_ROOT="$PWD"
    export PG_BIN=${postgresql}/bin
    export PG_CONFIG=${postgresql}/bin/pg_config
    export PGWRH_TEST_BIN_DIR=${postgresql}/bin
  '';
}

with import <nixpkgs> {};

let
  xpg = callPackage ./nix/xpg.nix { inherit fetchFromGitHub; };
  testgres = callPackage ./nix/testgres.nix {};

  postgresql = postgresql_18.withPackages (ps: [
    # The 0.2.1 upgrade test needs both the legacy and cookie-protected APIs.
    (ps.pg_background.overrideAttrs {
      version = "1.9.2";
      src = fetchFromGitHub {
        owner = "vibhorkum";
        repo = "pg_background";
        tag = "v1.9.2";
        hash = "sha256-R78lB/58/dfZPg4XZ5xGuQ/Ftv+SQzU4aeJop6tslK8=";
      };
    })
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
    # pgwrh_fdw includes libpq server headers with TLS and GSSAPI support.
    openssl
    libkrb5
    gnumake
  ];

  shellHook = ''
    export HISTFILE=.history
    export PGWRH_REPO_ROOT="$PWD"
    export PG_BIN=${postgresql}/bin
    export PG_CONFIG=${postgresql.pg_config}/bin/pg_config
    export PGWRH_TEST_BIN_DIR=${postgresql}/bin
  '';
}

# Complete UI test environment, including the external HTTP server.
let
  pkgs = import <nixpkgs> {};
  postgres = pkgs.postgresql_18.withPackages (ps: [ ps.pg_background ]);
  python = pkgs.python3.withPackages (ps: [ ps.pytest (pkgs.callPackage ./testgres.nix {}) ]);
in pkgs.mkShell {
  packages = [ postgres python pkgs.postgrest pkgs.gnumake pkgs.openssl pkgs.libkrb5 ];
  PG_CONFIG = "${postgres.pg_config}/bin/pg_config";
  PG_BIN = "${postgres}/bin";
  PGWRH_TEST_BIN_DIR = "${postgres}/bin";
}

# Shared integration-test dependencies; CI imports this through flake.lock.
{ pkgs, postgresql ? pkgs.postgresql_18, background ? pkgs.callPackage ./pg-background.nix { inherit postgresql; } }:
let
  postgres = postgresql.withPackages (ps: [ background ]);
  python = pkgs.python3.withPackages (ps: [ ps.pytest (pkgs.callPackage ./testgres.nix {}) ]);
in pkgs.mkShell {
  packages = [ postgres postgres.pg_config python pkgs.postgrest pkgs.gnumake pkgs.llvmPackages.clang ];
  buildInputs = [ pkgs.openssl pkgs.libkrb5 ];
  PG_CONFIG = "${postgres.pg_config}/bin/pg_config";
  PG_BIN = "${postgres}/bin";
  PGWRH_TEST_BIN_DIR = "${postgres}/bin";
}

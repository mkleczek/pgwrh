{
  description = "pgwrh 1.0.0 and PostgreSQL 18 with all required extensions";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  # Intel macOS is still supported by the 26.05 Darwin branch.
  inputs.nixpkgs-intel-darwin.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";
  outputs = { self, nixpkgs, nixpkgs-intel-darwin }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      packagesFor = system:
        let
          source = if system == "x86_64-darwin" then nixpkgs-intel-darwin else nixpkgs;
          pkgs = import source { inherit system; };
          pgwrh = pkgs.postgresql_18.pkgs.callPackage ./nix/pgwrh.nix { };
          postgresql = pkgs.postgresql_18.withPackages (ps: [ pgwrh ps.pg_background ]);
        in { inherit pkgs pgwrh postgresql; };
    in {
      packages = forAllSystems (system:
        let p = packagesFor system;
        in { inherit (p) pgwrh postgresql; default = p.postgresql; });
      devShells = forAllSystems (system:
        let p = packagesFor system;
        in {
          tests = import ./nix/tests.nix { pkgs = p.pkgs; };
          default = p.pkgs.mkShell {
          packages = [ p.postgresql p.postgresql.pg_config p.pkgs.python3 ];
          inputsFrom = [ p.pgwrh ];
          PG_CONFIG = "${p.postgresql.pg_config}/bin/pg_config";
        }; });
      checks = forAllSystems (system:
        let p = packagesFor system;
        in { installed = p.pkgs.runCommand "pgwrh-installed-1.0.0" {
          nativeBuildInputs = [ p.postgresql ];
        } ''
          bash ${./test/packaging/installed.sh} ${./test/packaging/installed.sql}
          touch "$out"
        ''; });
      nixosModules.default = { config, lib, ... }: {
        options.services.pgwrh.enable = lib.mkEnableOption "pgwrh on PostgreSQL 18";
        config = lib.mkIf config.services.pgwrh.enable {
          services.postgresql = {
            enable = true;
            package = self.packages.${config.nixpkgs.hostPlatform.system}.postgresql;
            settings = {
              shared_preload_libraries = "pgwrh_wait";
              wal_level = "logical";
              max_worker_processes = lib.mkDefault 32;
              max_replication_slots = lib.mkDefault 32;
              max_wal_senders = lib.mkDefault 32;
              max_logical_replication_workers = lib.mkDefault 16;
            };
          };
        };
      };
    };
}

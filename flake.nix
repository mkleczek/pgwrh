{
  description = "pgwrh 1.0.0-alpha1 and PostgreSQL 18/19 with all required extensions";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  # Intel macOS is still supported by the 26.05 Darwin branch.
  inputs.nixpkgs-intel-darwin.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";
  outputs = { self, nixpkgs, nixpkgs-intel-darwin }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      packagesFor = system: major:
        let
          source = if system == "x86_64-darwin" then nixpkgs-intel-darwin else nixpkgs;
          pkgs = import source { inherit system; };
          server = if major == "18" then pkgs.postgresql_18 else import ./nix/postgresql-19.nix { inherit pkgs; };
          background = pkgs.callPackage ./nix/pg-background.nix { postgresql = server; };
          pgwrh = server.pkgs.callPackage ./nix/pgwrh.nix { };
          postgresql = server.withPackages (ps: [ pgwrh background ]);
        in { inherit pkgs pgwrh postgresql server background; };
    in {
      packages = forAllSystems (system:
        let p18 = packagesFor system "18"; p19 = packagesFor system "19";
        in {
          inherit (p18) pgwrh postgresql;
          default = p18.postgresql;
          pgwrh-18 = p18.pgwrh; postgresql-18 = p18.postgresql;
          pgwrh-19 = p19.pgwrh; postgresql-19 = p19.postgresql;
        });
      devShells = forAllSystems (system:
        let
          p18 = packagesFor system "18"; p19 = packagesFor system "19";
          tests = p: import ./nix/tests.nix {
            inherit (p) pkgs background; postgresql = p.server;
          };
        in {
          tests = tests p18;
          tests-18 = tests p18;
          tests-19 = tests p19;
          default = p18.pkgs.mkShell {
            packages = [ p18.postgresql p18.postgresql.pg_config p18.pkgs.python3 ];
            inputsFrom = [ p18.pgwrh ];
            PG_CONFIG = "${p18.postgresql.pg_config}/bin/pg_config";
          };
        });
      checks = forAllSystems (system:
        let
          installed = major:
            let p = packagesFor system major;
            in p.pkgs.runCommand "pgwrh-installed-1.0.0-alpha1-pg${major}" {
              nativeBuildInputs = [ p.postgresql ];
            } ''
              bash ${./test/packaging/installed.sh} ${./test/packaging/installed.sql}
              touch "$out"
            '';
        in { installed = installed "18"; installed-19 = installed "19"; });
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

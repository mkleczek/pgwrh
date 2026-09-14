{
  description = "Simple flake to set up env";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flakelight.url = "github:nix-community/flakelight";
  };

  outputs = { flakelight, nixpkgs, ... }:
    flakelight ./. ({lib, ...}: {
      inputs.nixpkgs = nixpkgs;
      systems = lib.systems.flakeExposed;
      package = { stdenv, defaultMeta, pkgs }:
        stdenv.mkDerivation {
          pname = "pgwrh";
          version = "0.2.0";
          src = ./.;
          buildInputs = [ pkgs.coreutils pkgs.postgresql ];
          buildPhase = ''
            # The existing lock file predates PostgreSQL 18.
            USEPGXS=1 make WITH_LSN_WAIT=0 WITH_FDW=0 DESTDIR=$out all
          '';
          makeFlags = [ "WITH_LSN_WAIT=0" "WITH_FDW=0" ];
          meta = defaultMeta;
        };

      devShell.packages = pkgs: with pkgs; [ coreutils postgresql ];
    });
}

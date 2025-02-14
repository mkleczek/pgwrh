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
            USEPGXS=1 make DESTDIR=$out all
          '';
          meta = defaultMeta;
        };

      devShell.packages = pkgs: with pkgs; [ coreutils postgresql ];
    });
}

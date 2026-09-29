# Pin the PostgreSQL 19 preview consistently across the locked nixpkgs inputs.
{ pkgs }:
let
  generic = import (pkgs.path + "/pkgs/servers/sql/postgresql/generic.nix");
  source = {
    version = "19.0-beta3";
    rev = "3638289fb57bdabec00deda98ee9624a35f5d66a";
    hash = "sha256-Mcf46ksAzOn9hqCiF5kUwNskfFCBxrzqvznnxdwHfnM=";
  };
  options = { self = pkgs; };
in
# The Intel Darwin input predates generic.nix's uncurrying.
if builtins.functionArgs generic ? self then pkgs.callPackage generic (source // options)
else generic source options

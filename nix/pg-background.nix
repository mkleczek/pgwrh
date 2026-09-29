# Use the same tested pg_background release on every PostgreSQL major.
{ postgresql, fetchFromGitHub }:
postgresql.pkgs.pg_background.overrideAttrs {
  version = "2.0.3";
  src = fetchFromGitHub {
    owner = "vibhorkum";
    repo = "pg_background";
    tag = "v2.0.3";
    hash = "sha256-ZYBHUN+/fYSiozuZNVMFUZiAZl3qP4LOEdxeGVA+rmg=";
  };
}

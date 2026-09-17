# Nix and NixOS

The optional `pgwrh_ui` console is packaged with its assets and deployment
examples. Enable it only in the controller database with
`CREATE EXTENSION pgwrh_ui;` and run PostgREST separately. See the
[UI deployment guide](../pgwrh_ui/README.md) for installed file locations and setup.

The flake pins Nixpkgs and PostgreSQL 18. Its default package is PostgreSQL with
`pg_background`, `pgwrh`, `pgwrh_ui`, `pgwrh_fdw`, and `pgwrh_wait` available.
The four pgwrh extensions share version 1.0.0. Nix must have flakes enabled.

From a release checkout:

```sh
nix build
nix develop
nix flake check
```

`result/bin` contains the PostgreSQL tools. `nix build .#pgwrh` builds only the
extension files; `nix build .#postgresql` builds the complete server environment.
The installed-package check initializes a temporary database, preloads
`pgwrh_wait`, tests its standalone API, then explicitly installs `pgwrh` and its
dependencies. It checks both installation orders, removal of the core while the
wait API remains usable, all four extension versions, and both native libraries.
It verifies that the controller uses `pgwrh_fdw` without activating `postgres_fdw`.

For NixOS, add the release as an input to the system flake:

```nix
inputs.pgwrh.url = "github:mkleczek/pgwrh/v1.0.0";
```

Import `inputs.pgwrh.nixosModules.default` into the host modules and set
`services.pgwrh.enable = true`. The module uses the release's pinned PostgreSQL
bundle and configures logical replication and preloading. Enable it when
provisioning a PostgreSQL 18 cluster; normal PostgreSQL data-directory and
authentication configuration still applies. The release URL becomes available
when the tag is published. No third-party binary cache is required or configured.

When running the bundle outside NixOS, set `wal_level = logical`, append
`pgwrh_wait` to `shared_preload_libraries`, and restart PostgreSQL. The NixOS
module supplies these settings. See [native installation](packages.md) for
worker capacity and database configuration.

Create the extensions explicitly in each database as its administrator:

```sql
CREATE EXTENSION pgwrh CASCADE;
CREATE EXTENSION pgwrh_wait;
```

For the standalone wait API, create only `pgwrh_wait`; it does not require the
`pgwrh` extension or its dependencies.

This is a fresh installation release; it does not upgrade an existing pgwrh
database. For custom PostgreSQL environments, use
`postgresql_18.pkgs.callPackage ./nix/pgwrh.nix { }` and include the result together
with `pg_background` in `postgresql_18.withPackages`.

# Nix and NixOS

The default Nix package provides PostgreSQL 18, all four [pgwrh
extensions](../README.md#components) at version 1.0.0-alpha1, and their `pg_background`
dependency. Nix must have flakes enabled.

From a release checkout:

```sh
nix build
nix develop
```

`result/bin` contains the PostgreSQL tools. `nix build .#pgwrh` builds only the
extension files; `nix build .#postgresql` builds the complete server
environment.

For NixOS, add the release as an input to the system flake:

```nix
inputs.pgwrh.url = "github:mkleczek/pgwrh/v1.0.0-alpha1";
```

Import `inputs.pgwrh.nixosModules.default` into the host modules and set
`services.pgwrh.enable = true`. The module uses the release's pinned PostgreSQL
bundle and configures logical replication and preloading. Enable it when
provisioning a PostgreSQL 18 cluster; normal PostgreSQL data-directory and
authentication configuration still applies. The release URL becomes available
when the tag is published. No third-party binary cache is required or
configured.

When running the bundle outside NixOS, set `wal_level = logical`, append
`pgwrh_wait` to `shared_preload_libraries`, and restart PostgreSQL. The NixOS
module supplies these settings. See [native installation](packages.md) for
worker capacity and database configuration.

Create the extensions explicitly in the controller and each replica database as
its administrator:

```sql
CREATE EXTENSION pgwrh CASCADE;
-- Optional, for subscribers using replication visibility barriers:
CREATE EXTENSION pgwrh_wait;
```

For the standalone wait API, create only `pgwrh_wait`; it does not require the
`pgwrh` extension or its dependencies.

To enable the optional console, create `pgwrh_ui` only in the controller
database and run PostgREST separately. See [console
setup](../pgwrh_ui/README.md).

This is a fresh installation release; it does not upgrade an existing pgwrh
database. For custom PostgreSQL environments, use
`postgresql_18.pkgs.callPackage ./nix/pgwrh.nix { }` and include the result
together with `pg_background` in `postgresql_18.withPackages`.

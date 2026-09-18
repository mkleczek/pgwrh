# Contributor documentation

For installation and cluster operation, start with the [project
guide](../../README.md) and [cluster concepts](../overview.md). This directory
collects implementation notes, test instructions and historical validation
records.

## Build and test

- [Build and package the extensions](../packaging.md).
- [Run the test suites](testing.md).
- [Prepare and publish a release](../releasing.md).

## Implementation guides

- [Shard placement algorithm](placement.md).
- [Replica routing and shard handoff](replica-routing.md).
- [Replication visibility monitor](lsn-wait.md).
- [Controller console](controller-ui.md).
- [FDW transaction context and maintenance](../../pgwrh_fdw/docs/design.md).
- [FDW virtual-server routing](../../pgwrh_fdw/docs/virtual-server-internals.md).
- [FDW upstream provenance](../../pgwrh_fdw/UPSTREAM.md).

## Historical records

These records describe the revisions and environments named in each document;
they are not evidence that the current release has passed validation.

- [Packaging validation](packaging-validation.md).
- [Integration audit](pgwrh-next-integration.md).
- [FDW validation](../../pgwrh_fdw/docs/validation.md).
- [Earlier shard-routing proposal](../../pgwrh_fdw/docs/shard-routing-proposal.md).

## Repository layout

```text
pgwrh/          SQL extension: control file, SQL sources, and Makefile
pgwrh_ui/       Optional SQL controller console served by external PostgREST
pgwrh_wait/     Replication wait extension: control file, SQL, C sources, and Makefile
pgwrh_fdw/      Foreign data wrapper: sources, control file, SQL, docs, and Makefile
test/
  pgwrh/        Controller and replica integration tests
  pgwrh_ui/     Console SQL and HTTP tests
  pgwrh_wait/   Replication wait tests
  pgwrh_fdw/    FDW integration, SQL, isolation, and TAP tests
  check-install.py
Makefile        Combined build, install, clean, and test entry points
flake.nix       Complete PostgreSQL 18 bundle, extension package, and NixOS module
flake.lock
shell.nix       PostgreSQL 18 integration-test environment
nix/            Supporting Nix expressions
packaging/      RPM, DEB, container, and signed repository build tooling
examples/compose/  Controller and two-replica demonstration
docs/           Project documentation
```

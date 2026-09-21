# Containers and the local demonstration cluster

The image includes PostgreSQL 18, the four [pgwrh
extensions](../README.md#components) at version 1.0.0-alpha1, and their `pg_background`
dependency. It configures logical replication and preloads `pgwrh_wait`.

Images built from the development branch additionally include the optional
`pgwrh_gist_extra` extension. Enable it explicitly with
`CREATE EXTENSION pgwrh_gist_extra CASCADE`; the image does not activate it.

When initializing an empty volume, it enables `pgwrh` with its dependencies and
`pgwrh_wait` in `POSTGRES_DB`. Existing volumes are not initialized again. The
Compose quickstart adds a controller-only browser console served by PostgREST.
See [cluster concepts](overview.md) for the roles of the controller and
replicas.

## Try sharding locally

Install Docker with Compose and curl. From a repository checkout or unpacked
1.0.0-alpha1 source archive, run the command below. If the release image has not yet
been published, [build a local image](#build-a-local-image) first.

```sh
bash examples/compose/quickstart.sh
```

Wait for **Quickstart verified**. The setup service creates a controller, two
logical replicas, four hash partitions, and 100 example rows. It waits for
rollout readiness, commits the configuration, and compares query results on both
replicas.

```sh
docker compose -f examples/compose/compose.yaml exec replica1 psql -U postgres -d pgwrh_demo \
  -c 'SELECT count(*), sum(id) FROM demo.events;'
```

Expected: `100` rows and sum `5050`. Repeat on `replica2` for the same result.
The controller is available on localhost:15432; replicas on localhost:15433 and
15434. Database: `pgwrh_demo`, user: `postgres`, password: `pgwrh-local-demo`.
These fixed credentials are for this local demo. Do not expose it as a production
service. Override `PGWRH_CONTROLLER_PORT`, `PGWRH_REPLICA1_PORT`, and
`PGWRH_REPLICA2_PORT` if those host ports are occupied.

Open [the console](http://localhost:13000/rpc/index?group_id=demo) for the
read-only console served by PostgREST 14.16. Override `PGWRH_UI_PORT` to change
its loopback port. The `ui` Compose profile enables `pgwrh_ui` only on the
controller and grants the viewer role to a dedicated login; it does not enable
anonymous operator access. For a cluster without the console, use `docker
compose up -d` in `examples/compose` without the `ui` profile.

From `examples/compose`, `docker compose --profile ui down` retains database
volumes. Running setup again reuses the demo configuration and restarts replica
reconciliation. To delete the demo data and start fresh, use `docker compose
--profile ui down -v`.

## Build a local image

From the repository root:

```sh
docker build -f packaging/container/Dockerfile -t pgwrh:1.0.0-alpha1-local .
PGWRH_IMAGE=pgwrh:1.0.0-alpha1-local bash examples/compose/quickstart.sh
```

`PGWRH_IMAGE` also accepts an immutable image digest. The default
`ghcr.io/mkleczek/pgwrh:1.0.0-alpha1-pg18` reference becomes available when the release
workflow publishes it; use the local build before then.

The image follows the [official PostgreSQL
image](https://hub.docker.com/_/postgres) entrypoint conventions. PostgreSQL 18
volumes mount at `/var/lib/postgresql`. Production deployments should supply
their own credentials, storage, resource limits, replication settings and
network configuration. Overriding the image's command replaces its default
PostgreSQL settings; preserve the required preload and logical-replication
settings. See [native installation](packages.md).

## PostgreSQL 19 preview

From the development checkout, build a local PostgreSQL 19 Beta 3 image:

```sh
docker build -f packaging/container/Dockerfile --build-arg PG_MAJOR=19 \
  --build-arg POSTGRES_IMAGE=postgres:19beta3-trixie -t pgwrh:pg19-preview .
PGWRH_IMAGE=pgwrh:pg19-preview bash examples/compose/quickstart.sh
```

Use fresh demo volumes for the different server major. The release workflow
prepares a separate `-pg19` image tag; the already published alpha1 tag remains
unchanged. Both development images include pg_background 2.0.3 or newer.

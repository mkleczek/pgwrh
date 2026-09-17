# Containers and the local demonstration cluster

The optional `pgwrh_ui` console is packaged with its assets and deployment
examples. Enable it only in the controller database with
`CREATE EXTENSION pgwrh_ui;` and run PostgREST separately. See the
[UI deployment guide](../pgwrh_ui/README.md) for installed file locations and setup.

The image contains PostgreSQL 18, `pg_background`, and all four
pgwrh extensions at version 1.0.0. It enables logical replication and preloads
`pgwrh_wait`. During initialization of an empty volume it runs
`CREATE EXTENSION pgwrh CASCADE` and `CREATE EXTENSION pgwrh_wait` in `POSTGRES_DB`,
enabling the core with its dependencies and the independent wait API.
Existing volumes are not initialized again.

## Try sharding locally

Install Docker with Compose and curl. Download and unpack the 1.0.0 source archive, then run from its root:

```sh
bash examples/compose/quickstart.sh
```

Wait for **Quickstart verified**. The setup service creates a controller, two logical
replicas, four hash partitions, and 100 example rows. It waits for rollout
readiness, commits the configuration, and compares query results on both replicas.

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

Open <http://localhost:13000/rpc/index?group_id=demo> for the read-only console
served by PostgREST 14.16. Override `PGWRH_UI_PORT` to change its loopback port.
The `ui` Compose profile enables `pgwrh_ui` only on the controller and grants the
viewer role to a dedicated login; it does not enable anonymous operator access.
For a cluster without the console, use `docker compose up -d` in
`examples/compose` without the `ui` profile.

From `examples/compose`, `docker compose --profile ui down` retains database volumes. Running setup again reuses the
demo configuration and restarts replica reconciliation. To delete the demo data
and start fresh, use `docker compose --profile ui down -v`.

## Build and test before publication

From the repository root:

```sh
docker build -f packaging/container/Dockerfile -t pgwrh:1.0.0-local .
PGWRH_IMAGE=pgwrh:1.0.0-local bash examples/compose/quickstart.sh
```

The build tests the installed extensions in a temporary database before producing
an image. `PGWRH_IMAGE` also permits using an immutable image digest. The default
`ghcr.io/mkleczek/pgwrh:1.0.0-pg18` reference becomes available when the release
workflow publishes it; a local build is required before then.

The image follows the [official PostgreSQL image](https://hub.docker.com/_/postgres)
entrypoint conventions. PostgreSQL 18 volumes mount at `/var/lib/postgresql`.
Production deployments should supply their own credentials, storage, resource
limits, replication settings and network configuration. Overriding the image's
command replaces its default PostgreSQL settings; preserve the required preload
and logical-replication settings. See [native installation](packages.md).

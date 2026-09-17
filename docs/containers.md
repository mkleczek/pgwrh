# Containers and the local demonstration cluster

The image contains PostgreSQL 18, `pg_background`, and all three
pgwrh extensions at version 0.3.0. It enables logical replication and preloads
`pgwrh_wait`. During initialization of an empty volume it runs
`CREATE EXTENSION pgwrh CASCADE` and `CREATE EXTENSION pgwrh_wait` in `POSTGRES_DB`,
enabling the core with its dependencies and the independent wait API.
Existing volumes are not initialized again.

## Try sharding locally

Download and unpack the 0.3.0 source archive, then:

```sh
cd examples/compose
docker compose up -d
docker compose logs -f setup
```

Wait for **pgwrh demo ready**. The setup service creates a controller, two logical
replicas, four hash partitions, and 100 example rows. It waits for rollout
readiness, commits the configuration, and compares query results on both replicas.

```sh
docker compose exec replica1 psql -U postgres -d pgwrh_demo \
  -c 'SELECT count(*), sum(id) FROM demo.events;'
```

Expected: `100` rows and sum `5050`. Repeat on `replica2` for the same result.
The controller is available on localhost:15432; replicas on localhost:15433 and
15434. Database: `pgwrh_demo`, user: `postgres`, password: `pgwrh-local-demo`.
These fixed credentials are for this local demo. Do not expose it as a production
service. Override `PGWRH_CONTROLLER_PORT`, `PGWRH_REPLICA1_PORT`, and
`PGWRH_REPLICA2_PORT` if those host ports are occupied.

`docker compose down` retains database volumes. Running setup again reuses the
demo configuration and restarts replica reconciliation. To delete the demo data
and start fresh, use `docker compose down -v`.

## Build and test before publication

From the repository root:

```sh
docker build -f packaging/container/Dockerfile -t pgwrh:0.3.0-local .
PGWRH_IMAGE=pgwrh:0.3.0-local docker compose -f examples/compose/compose.yaml up -d
PGWRH_IMAGE=pgwrh:0.3.0-local docker compose -f examples/compose/compose.yaml logs -f setup
```

The build tests the installed extensions in a temporary database before producing
an image. `PGWRH_IMAGE` also permits using an immutable image digest. The default
`ghcr.io/mkleczek/pgwrh:0.3.0-pg18` reference becomes available when the release
workflow publishes it; a local build is required before then.

The image follows the [official PostgreSQL image](https://hub.docker.com/_/postgres)
entrypoint conventions. PostgreSQL 18 volumes mount at `/var/lib/postgresql`.
Production deployments should supply their own credentials, storage, resource
limits, replication settings and network configuration. Overriding the image's
command replaces its default PostgreSQL settings; preserve the required preload
and logical-replication settings. See [native installation](packages.md).

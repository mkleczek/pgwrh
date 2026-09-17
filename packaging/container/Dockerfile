# syntax=docker/dockerfile:1
ARG POSTGRES_IMAGE=postgres:18.6-trixie
FROM ${POSTGRES_IMAGE} AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential postgresql-server-dev-18 libssl-dev libkrb5-dev python3
WORKDIR /src
COPY . .
RUN make -j2 PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config with_llvm=no \
    && make test-packaging PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config with_llvm=no \
    && make install PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config with_llvm=no DESTDIR=/stage
FROM ${POSTGRES_IMAGE} AS runtime
LABEL org.opencontainers.image.source="https://github.com/mkleczek/pgwrh" \
      org.opencontainers.image.version="0.3.0" \
      org.opencontainers.image.description="PostgreSQL 18 with pgwrh, pgwrh_fdw, pgwrh_wait and pg_background"
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-18-pg-background \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /stage/ /
COPY LICENSE /usr/share/doc/pgwrh/LICENSE
COPY pgwrh_fdw/COPYRIGHT /usr/share/doc/pgwrh/FDW-COPYRIGHT
COPY packaging/container/init.sql /docker-entrypoint-initdb.d/10-pgwrh.sql
COPY test/packaging /tmp/pgwrh-packaging-test
RUN gosu postgres bash /tmp/pgwrh-packaging-test/installed.sh \
    && rm -rf /tmp/pgwrh-packaging-test
CMD ["postgres", "-c", "shared_preload_libraries=pgwrh_wait", "-c", "wal_level=logical", "-c", "max_worker_processes=32", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32", "-c", "max_logical_replication_workers=16"]

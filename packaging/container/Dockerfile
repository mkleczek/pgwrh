# syntax=docker/dockerfile:1
ARG POSTGRES_IMAGE=postgres:18.6-trixie
FROM ${POSTGRES_IMAGE} AS build
ARG PG_MAJOR=18
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential postgresql-server-dev-${PG_MAJOR} libssl-dev libkrb5-dev python3
WORKDIR /src
COPY . .
RUN make -j2 PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config with_llvm=no \
    && make test-packaging PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config with_llvm=no \
    && make install PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config with_llvm=no DESTDIR=/stage
FROM ${POSTGRES_IMAGE} AS runtime
ARG PG_MAJOR=18
LABEL org.opencontainers.image.source="https://github.com/mkleczek/pgwrh" \
      org.opencontainers.image.version="1.0.0-alpha1" \
      org.opencontainers.image.description="PostgreSQL ${PG_MAJOR} with pgwrh, pgwrh_ui, pgwrh_fdw, pgwrh_wait and pg_background"
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-${PG_MAJOR}-pg-background \
    && dpkg-query -W -f='${Version}\n' "postgresql-${PG_MAJOR}-pg-background" > /tmp/background-version \
    && dpkg --compare-versions "$(cat /tmp/background-version)" ge 2.0.3 \
    && rm /tmp/background-version \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /stage/ /
COPY LICENSE /usr/share/doc/pgwrh/LICENSE
COPY pgwrh_fdw/${PG_MAJOR}/COPYRIGHT /usr/share/doc/pgwrh/FDW-COPYRIGHT
COPY packaging/container/init.sql /docker-entrypoint-initdb.d/10-pgwrh.sql
COPY test/packaging /tmp/pgwrh-packaging-test
RUN gosu postgres bash /tmp/pgwrh-packaging-test/installed.sh \
    && rm -rf /tmp/pgwrh-packaging-test
CMD ["postgres", "-c", "shared_preload_libraries=pgwrh_wait", "-c", "wal_level=logical", "-c", "max_worker_processes=32", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32", "-c", "max_logical_replication_workers=16"]

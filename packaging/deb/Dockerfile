# syntax=docker/dockerfile:1
ARG BASE_IMAGE=ubuntu:24.04
FROM ${BASE_IMAGE} AS repository
ARG PG_MAJOR=18
ARG PGDG_COMPONENTS=main
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl postgresql-common \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl --fail --silent --show-error https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && . /etc/os-release \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg ${PGDG_COMPONENTS}" > /etc/apt/sources.list.d/pgdg.list \
    && echo 'create_main_cluster = false' >> /etc/postgresql-common/createcluster.conf
FROM repository AS build
WORKDIR /build/pgwrh
COPY packaging/deb/debian ./debian
COPY packaging/deb/select-major.py /tmp/select-major.py
RUN apt-get update && apt-get install -y --no-install-recommends build-essential dpkg-dev python3 \
    && python3 /tmp/select-major.py "$PG_MAJOR" debian \
    && apt-get build-dep -y .
COPY . .
RUN . /etc/os-release \
    && sed -i "1s/) /+${VERSION_CODENAME}) /" debian/changelog \
    && dpkg-buildpackage -us -uc -b \
    && mkdir /artifacts \
    && find /build -maxdepth 1 -type f \( -name '*.deb' -o -name '*.ddeb' -o -name '*.changes' -o -name '*.buildinfo' \) \
        -exec cp '{}' /artifacts/ \;
FROM repository AS verify
COPY --from=build /artifacts /artifacts
COPY test/packaging /test
RUN apt-get update && apt-get install -y --no-install-recommends /artifacts/postgresql-${PG_MAJOR}-pgwrh_*.deb \
    && runuser -u postgres -- env PATH="/usr/lib/postgresql/${PG_MAJOR}/bin:$PATH" bash /test/installed.sh
FROM scratch AS artifacts
COPY --from=verify /artifacts/ /

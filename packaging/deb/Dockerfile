# syntax=docker/dockerfile:1
ARG BASE_IMAGE=ubuntu:24.04
FROM ${BASE_IMAGE} AS repository
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl postgresql-common \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl --fail --silent --show-error https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && . /etc/os-release \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && echo 'create_main_cluster = false' >> /etc/postgresql-common/createcluster.conf
FROM repository AS build
WORKDIR /build/pgwrh
COPY packaging/deb/debian ./debian
RUN apt-get update && apt-get install -y --no-install-recommends build-essential dpkg-dev \
    && apt-get build-dep -y .
COPY . .
RUN . /etc/os-release \
    && sed -i "1s/1.0.0-1/1.0.0-1+${VERSION_CODENAME}/" debian/changelog \
    && dpkg-buildpackage -us -uc -b \
    && mkdir /artifacts \
    && find /build -maxdepth 1 -type f \( -name '*.deb' -o -name '*.ddeb' -o -name '*.changes' -o -name '*.buildinfo' \) \
        -exec cp '{}' /artifacts/ \;
FROM repository AS verify
COPY --from=build /artifacts /artifacts
COPY test/packaging /test
RUN apt-get update && apt-get install -y --no-install-recommends /artifacts/postgresql-18-pgwrh_*.deb \
    && runuser -u postgres -- env PATH="/usr/lib/postgresql/18/bin:$PATH" bash /test/installed.sh
FROM scratch AS artifacts
COPY --from=verify /artifacts/ /

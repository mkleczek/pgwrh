# syntax=docker/dockerfile:1
FROM almalinux:9 AS repository
ARG TARGETARCH
RUN dnf install -y dnf-plugins-core epel-release \
    && dnf config-manager --set-enabled crb \
    && case "$TARGETARCH" in amd64) arch=x86_64;; arm64) arch=aarch64;; *) exit 1;; esac \
    && dnf install -y "https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-${arch}/pgdg-redhat-repo-latest.noarch.rpm" \
    && dnf -qy module disable postgresql
FROM repository AS build
ARG WITH_LLVM=1
RUN dnf install -y rpm-build pgdg-srpm-macros tar gzip
COPY packaging/rpm/pgwrh.spec /tmp/pgwrh.spec
RUN dnf builddep -y --define 'pgmajorversion 18' --define "llvm ${WITH_LLVM}" /tmp/pgwrh.spec
WORKDIR /build/pgwrh
COPY . .
RUN mkdir -p /root/rpmbuild/SOURCES \
    && tar -czf /root/rpmbuild/SOURCES/pgwrh-1.0.0.tar.gz --transform='s,^\.,pgwrh-1.0.0,' . \
    && rpmbuild -ba --define 'pgmajorversion 18' --define "llvm ${WITH_LLVM}" packaging/rpm/pgwrh.spec \
    && mkdir /artifacts && cp /root/rpmbuild/RPMS/*/*.rpm /root/rpmbuild/SRPMS/*.rpm /artifacts/
FROM repository AS verify
ARG WITH_LLVM=1
COPY --from=build /artifacts /artifacts
COPY test/packaging /test
RUN rpm_arch=$(rpm --eval '%{_arch}') \
    && dnf install -y /artifacts/pgwrh_18-1.0.0-*."$rpm_arch".rpm \
    && if [ "$WITH_LLVM" = 1 ]; then dnf install -y /artifacts/pgwrh_18-llvmjit-1.0.0-*."$rpm_arch".rpm; fi \
    && runuser -u postgres -- env PATH="/usr/pgsql-18/bin:$PATH" bash /test/installed.sh
FROM scratch AS artifacts
COPY --from=verify /artifacts/ /

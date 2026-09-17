FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils rpm createrepo-c gnupg python3 ca-certificates
COPY packaging/repository/build.py /build.py
COPY VERSION /VERSION
COPY test/packaging/repository.sh /test.sh
CMD ["bash", "/test.sh"]

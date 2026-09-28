FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils rpm createrepo-c gnupg python3 python3-rpm ca-certificates
COPY packaging /packaging
COPY VERSION /VERSION
COPY test/packaging/repository.sh /test.sh
CMD ["bash", "/test.sh"]

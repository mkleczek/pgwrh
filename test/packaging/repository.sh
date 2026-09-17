#!/usr/bin/env bash
# Run only in the disposable repository-test container (modifies its APT trust).
set -euo pipefail
export GNUPGHOME
gnupg_root=$(mktemp -d)
GNUPGHOME=$gnupg_root
chmod 700 "$GNUPGHOME"
trap 'gpgconf --kill all; rm -rf "$GNUPGHOME"' EXIT
gpg --batch --passphrase '' --quick-generate-key 'pgwrh packaging test <test@example.invalid>' rsa2048 sign 1d
fingerprint=$(gpg --with-colons --list-secret-keys | awk -F: '$1 == "fpr" { print $10; exit }')
python3 /build.py /artifacts /repository --key "$fingerprint"
gpg --dearmor < /repository/pgwrh.asc > /usr/share/keyrings/pgwrh-test.gpg
for release in /repository/apt/dists/*/InRelease; do
    gpg --verify "$release"
    distro=$(basename "$(dirname "$release")")
    echo "deb [signed-by=/usr/share/keyrings/pgwrh-test.gpg] file:/repository/apt $distro main" >> /etc/apt/sources.list.d/pgwrh-test.list
done
apt-get update
apt-cache policy postgresql-18-pgwrh | tee /tmp/policy
# APT must see a candidate from the signed local repository.
version=$(cat /VERSION)
grep -F "Candidate: $version-" /tmp/policy
rpm --import /repository/pgwrh.asc
for package in /repository/rpm/el9/*/*.rpm; do
    rpm --checksig "$package" | tee /tmp/signature
    grep -q 'signatures OK' /tmp/signature
done
for metadata in /repository/rpm/el9/*/repodata/repomd.xml; do
    gpg --verify "$metadata.asc" "$metadata"
done

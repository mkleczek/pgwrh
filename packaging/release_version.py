"""Translate an upstream release version into package and publication metadata."""
import argparse
import json
from pathlib import Path
import re


def release_metadata(version):
    # Keep stage and sequence explicit; package managers must order prereleases
    # below the final version. Build metadata and arbitrary suffixes are rejected.
    number = r'(?:0|[1-9][0-9]*)'
    match = re.fullmatch(
        rf'({number}\.{number}\.{number})(?:-((?:alpha|beta|rc)[1-9][0-9]*))?', version)
    if not match:
        raise ValueError(f'Invalid release version: {version!r}; use X.Y.Z[-alphaN|-betaN|-rcN]')
    base, suffix = match.groups()
    return {
        'version': version,
        'package_version': base + ('~' + suffix if suffix else ''),
        'prerelease': bool(suffix),
    }


def validate_release(metadata, tag='', github_prerelease=None):
    if tag and tag != 'v' + metadata['version']:
        raise ValueError('Release tag differs from VERSION')
    if github_prerelease is not None and github_prerelease != metadata['prerelease']:
        raise ValueError('GitHub prerelease status differs from VERSION')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', default=None)
    parser.add_argument('--field', choices=('version', 'package_version', 'prerelease'))
    args = parser.parse_args()
    value = args.version
    if value is None:
        value = (Path(__file__).resolve().parents[1] / 'VERSION').read_text().strip()
    metadata = release_metadata(value)
    if args.field:
        result = metadata[args.field]
        print(str(result).lower() if isinstance(result, bool) else result)
    else:
        print(json.dumps(metadata))

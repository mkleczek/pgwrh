#!/usr/bin/env python3
"""Select the target major in a disposable Debian packaging tree."""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('major', choices=('18', '19'))
parser.add_argument('debian', type=Path)
args = parser.parse_args()
for name in ('control', 'rules', 'postgresql-18-pgwrh.install'):
    source = args.debian / name
    text = source.read_text().replace('postgresql-18', 'postgresql-' + args.major)
    text = text.replace('postgresql/18/', 'postgresql/' + args.major + '/')
    text = text.replace('postgresql-server-dev-18', 'postgresql-server-dev-' + args.major)
    text = text.replace('PostgreSQL 18', 'PostgreSQL ' + args.major)
    target = args.debian / name.replace('postgresql-18', 'postgresql-' + args.major)
    target.write_text(text)
    if source != target:
        source.unlink()

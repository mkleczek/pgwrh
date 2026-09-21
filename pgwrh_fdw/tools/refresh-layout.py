#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Create or refresh a pure directory-move change above a standalone FDW aggregate."""
import argparse
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('major', choices=('18', '19'))
    parser.add_argument('--repository', default='.', help='jj workspace (default: current directory)')
    parser.add_argument('--aggregate', help='create a new move change above this aggregate')
    parser.add_argument('--check', action='store_true', help='verify the bookmarked layout without refreshing it')
    args = parser.parse_args()
    if args.check and args.aggregate:
        parser.error('--check cannot be combined with --aggregate')
    repo = str(Path(args.repository).resolve())

    def jj(*arguments):
        return command('jj', '-R', repo, *arguments)

    def resolve(revision):
        return jj('log', '-r', revision, '--no-graph', '-T', 'commit_id ++ "\\n"').splitlines()

    bookmark = 'fdw_base_' + args.major
    existing = resolve(f'bookmarks(exact:{bookmark})')
    if len(existing) > 1:
        sys.exit(f'{bookmark} is conflicted; resolve its bookmark first')
    if args.aggregate:
        revisions = resolve(args.aggregate)
        if len(revisions) != 1:
            sys.exit('--aggregate must identify exactly one revision')
        aggregate = revisions[0]
    else:
        if not existing:
            sys.exit(f'{bookmark} does not exist; specify --aggregate for the first directory move')
        parents = resolve(existing[0] + '-')
        if len(parents) != 1:
            sys.exit(f'{bookmark} must have exactly one aggregate parent; use --aggregate to create its directory move')
        aggregate = parents[0]
    if resolve(aggregate + ' & conflicts()'):
        sys.exit('resolve the aggregate conflicts before refreshing its directory move')

    files = jj('file', 'list', '-r', aggregate).splitlines()
    if not {'Makefile', 'postgres_fdw.c', 'pgwrh_fdw.control'}.issubset(files) or any(
        path.startswith('pgwrh_fdw/') for path in files
    ):
        sys.exit('the aggregate must contain the standalone FDW at its root')

    git_dir = jj('git', 'root')

    def git(*arguments):
        return command('git', '--git-dir=' + git_dir, *arguments)

    tree = git('rev-parse', aggregate + '^{tree}')

    def matches(revision):
        if resolve(revision + '-') != [aggregate] or resolve(revision + ' & conflicts()'):
            return False
        return (git('ls-tree', '--name-only', revision) == 'pgwrh_fdw'
                and git('ls-tree', '--name-only', revision + ':pgwrh_fdw') == args.major
                and git('rev-parse', revision + ':pgwrh_fdw/' + args.major) == tree)

    if existing and matches(existing[0]):
        print(f'{bookmark}: directory move matches its aggregate')
        return
    if args.check:
        sys.exit(f'{bookmark}: directory move is stale; run this helper without --check')

    # Build the exact move in a disposable workspace. jj, rather than Git
    # checkout/index operations, owns the working copies and descendant rebases.
    name = 'fdw-layout-' + uuid.uuid4().hex
    candidate = None
    added = False
    keep_candidate = False
    with tempfile.TemporaryDirectory(prefix='pgwrh-fdw-layout-') as temporary:
        workspace = Path(temporary) / 'workspace'
        try:
            jj('workspace', 'add', '--name', name, '--sparse-patterns', 'full', '-r', aggregate, str(workspace))
            added = True
            candidate = command('jj', '-R', str(workspace), 'log', '-r', '@', '--no-graph', '-T', 'change_id')
            entries = [entry for entry in workspace.iterdir() if entry.name != '.jj']
            destination = workspace / 'pgwrh_fdw' / args.major
            destination.mkdir(parents=True)
            for entry in entries:
                entry.rename(destination / entry.name)
            command('jj', '-R', str(workspace), 'describe', '-m',
                    f'Move PostgreSQL {args.major} FDW into pgwrh_fdw/{args.major}')
            candidate = command('jj', '-R', str(workspace), 'log', '-r', '@', '--no-graph', '-T', 'commit_id')
            if not matches(candidate):
                sys.exit('generated directory move does not match the aggregate')
            if args.aggregate:
                jj('bookmark', 'set', bookmark, '-r', candidate, '--allow-backwards')
                keep_candidate = True
            else:
                jj('restore', '--from', candidate, '--into', existing[0])
        finally:
            if added:
                jj('workspace', 'forget', name)
            if candidate and not keep_candidate:
                jj('abandon', candidate)
    if not matches(resolve(bookmark)[0]):
        sys.exit(f'{bookmark}: verification failed after refresh')
    print(f'{bookmark}: refreshed pgwrh_fdw/{args.major} from aggregate {aggregate[:12]}')


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(error.stderr.strip() if error.stderr else str(error))

#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise real jj directory moves, descendant rebases and source-tree equality."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HELPER = Path(__file__).resolve().parents[2] / 'pgwrh_fdw/tools/refresh-layout.py'


class LayoutTests(unittest.TestCase):
    def command(self, *args, check=True):
        result = subprocess.run(args, cwd=self.repo, env=self.env, text=True, capture_output=True)
        if check:
            self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def jj(self, *args):
        return self.command('jj', *args).stdout.strip()

    def revision(self, revision='@'):
        return self.jj('log', '-r', revision, '--no-graph', '-T', 'commit_id')

    def change(self):
        return self.jj('log', '-r', '@', '--no-graph', '-T', 'change_id')

    def write(self, name, content):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def refresh(self, major, *args, check=True):
        return self.command(sys.executable, str(HELPER), str(major), *args, check=check)

    def tree(self, revision):
        return self.command('git', '--git-dir=' + self.jj('git', 'root'),
                            'rev-parse', revision).stdout.strip()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='pgwrh-jj-layout-test-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.repo = root / 'repo'
        self.repo.mkdir()
        config = root / 'config.toml'
        config.write_text('[user]\nname = "FDW layout test"\nemail = "fdw-test@example.invalid"\n'
                          '[revset-aliases]\n"immutable_heads()" = "root()"\n')
        self.env = dict(os.environ, JJ_CONFIG=str(config), XDG_CONFIG_HOME=str(root / 'config'),
                        XDG_CACHE_HOME=str(root / 'cache'), GIT_CONFIG_NOSYSTEM='1',
                        GIT_CONFIG_GLOBAL=os.devnull)
        self.jj('git', 'init', '.')
        self.write('Makefile', '# standalone FDW\n')
        self.write('postgres_fdw.c', 'upstream\n')
        self.write('pgwrh_fdw.control', "default_version = 'test'\n")
        self.write('api.h', 'old API\n')
        self.write('test data/file with spaces', 'preserve this path\n')
        self.write('run.sh', '#!/bin/sh\nexit 0\n')
        (self.repo / 'run.sh').chmod(0o755)
        (self.repo / 'link').symlink_to('api.h')
        self.jj('describe', '-m', 'Common upstream')
        common = self.revision()
        self.jj('new', common, '-m', 'Shared behavior')
        self.write('postgres_fdw.c', 'upstream\nshared behavior\n')
        self.shared = self.change()
        self.jj('new', common, '-m', 'PostgreSQL 19 upstream')
        self.write('pg19-only.h', 'PostgreSQL 19\n')
        self.upstream19 = self.change()
        self.aggregates = {}
        for major, upstream in ((18, common), (19, self.upstream19)):
            self.jj('new', self.shared, upstream, '-m', f'Aggregate {major}')
            self.aggregates[major] = self.change()
            self.refresh(major, '--aggregate', self.aggregates[major])
        self.jj('new', 'root()', '-m', 'Project files')
        self.write('README', 'project documentation\n')
        self.write('Makefile', '# project build\n')
        project = self.revision()
        self.jj('new', '@', 'fdw_base_18', 'fdw_base_19', '-m', 'Integrate FDW layouts')
        # Resolve the initial standalone-root/project-root overlap explicitly.
        self.jj('restore', '--from', project)
        for major in (18, 19):
            self.jj('restore', '--from', f'fdw_base_{major}', f'pgwrh_fdw/{major}')
        self.jj('bookmark', 'set', 'main', '-r', '@')
        self.assertEqual(self.jj('log', '-r', 'ancestors(main) & conflicts()', '--no-graph'), '')
        self.jj('new', 'main')

    def assert_layout(self, major):
        bookmark = f'fdw_base_{major}'
        self.refresh(major, '--check')
        self.assertEqual(self.tree(self.revision(bookmark + '-') + '^{tree}'),
                         self.tree(self.revision('main') + f':pgwrh_fdw/{major}'))

    def test_exact_move_and_idempotence(self):
        before = self.revision('fdw_base_18')
        working = self.change()
        for major in (18, 19):
            self.assert_layout(major)
            self.refresh(major)
        self.assertEqual(self.revision('fdw_base_18'), before)
        self.assertEqual(self.change(), working)

    def test_shared_edits_additions_and_deletions_reach_main(self):
        self.jj('edit', self.shared)
        self.write('postgres_fdw.c', 'upstream\nshared correction\n')
        self.write('added.h', 'new API\n')
        (self.repo / 'api.h').unlink()
        self.jj('edit', 'main')
        self.jj('new', 'main')
        working = self.change()
        self.write('README', 'uncommitted user work\n')
        self.assertNotEqual(self.refresh(18, '--check', check=False).returncode, 0)
        for major in (18, 19):
            self.refresh(major)
            self.assert_layout(major)
            self.assertIn('shared correction', (self.repo / f'pgwrh_fdw/{major}/postgres_fdw.c').read_text())
            self.assertTrue((self.repo / f'pgwrh_fdw/{major}/added.h').exists())
            self.assertFalse((self.repo / f'pgwrh_fdw/{major}/api.h').exists())
        self.assertEqual((self.repo / 'README').read_text(), 'uncommitted user work\n')
        self.assertEqual((self.repo / 'Makefile').read_text(), '# project build\n')
        self.assertEqual(self.change(), working)
        self.assertEqual(self.jj('log', '-r', 'ancestors(@) & conflicts()', '--no-graph'), '')
        self.assertEqual(self.jj('workspace', 'list').count('\n'), 0)

    def test_upstream_update_keeps_other_major_and_shared_patch(self):
        before18 = self.revision('fdw_base_18')
        shared = self.revision(self.shared)
        self.jj('new', self.upstream19, '-m', 'Next PostgreSQL 19 upstream')
        self.write('connection.c', 'upstream fix\n')
        upstream = self.change()
        self.jj('rebase', '-s', self.aggregates[19], '-o', self.shared, '-o', upstream)
        self.jj('edit', 'main')
        self.refresh(19)
        self.assert_layout(19)
        self.assertEqual(self.revision('fdw_base_18'), before18)
        self.assertEqual(self.revision(self.shared), shared)
        self.assertTrue((self.repo / 'pgwrh_fdw/19/connection.c').exists())
        self.assertFalse((self.repo / 'pgwrh_fdw/18/connection.c').exists())

    def test_invalid_aggregate_does_not_move_bookmarks(self):
        before = [self.revision(f'fdw_base_{major}') for major in (18, 19)]
        result = self.refresh(18, '--aggregate', 'main', check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('standalone FDW', result.stderr)
        self.assertEqual([self.revision(f'fdw_base_{major}') for major in (18, 19)], before)


if __name__ == '__main__':
    unittest.main(verbosity=2)

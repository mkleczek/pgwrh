#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise filtered upstream updates and subsequent subtree integration."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

IMPORTER = Path(__file__).resolve().parents[2] / 'pgwrh_fdw/tools/import-upstream.py'


class UpstreamImportTests(unittest.TestCase):
    def git(self, repo, *args):
        result = subprocess.run(['git', '-C', str(repo), *args], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                env=dict(os.environ, FILTER_BRANCH_SQUELCH_WARNING='1'))
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def init(self, path):
        path.mkdir()
        self.git(path, 'init', '-b', 'main')
        self.git(path, 'config', 'user.name', 'FDW import test')
        self.git(path, 'config', 'user.email', 'fdw-test@example.invalid')

    def commit(self, repo, message):
        self.git(repo, 'add', '.')
        self.git(repo, 'commit', '-m', message)
        return self.git(repo, 'rev-parse', 'HEAD')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='fdw-import-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.pg = self.root / 'postgres'
        self.repo = self.root / 'pgwrh'
        self.init(self.pg)
        fdw = self.pg / 'contrib/postgres_fdw'
        fdw.mkdir(parents=True)
        (fdw / 'postgres_fdw.c').write_text('/* upstream baseline */\n')
        (self.pg / 'unrelated').write_text('not part of the import\n')
        self.commit(self.pg, 'Initial upstream FDW')
        self.git(self.pg, 'tag', 'REL_18_3')
        self.filtered = self.root / 'filtered'
        self.git(self.pg, 'clone', '--no-local', str(self.pg), str(self.filtered))
        for key in ('user.name', 'user.email'):
            self.git(self.filtered, 'config', key, self.git(self.pg, 'config', '--get', key))
        self.git(self.filtered, 'filter-branch', '--subdirectory-filter', 'contrib/postgres_fdw', '--', 'main')
        self.old_upstream = self.git(self.filtered, 'rev-parse', 'HEAD')
        with (self.filtered / 'postgres_fdw.c').open('a') as f:
            f.write('/* local patch */\n')
        self.patch = self.commit(self.filtered, 'Shared FDW behavior')
        self.base = self.git(self.filtered, 'commit-tree', 'HEAD^{tree}', '-p', self.old_upstream,
                             '-p', self.patch, '-m', 'Collect the functional patches')
        self.git(self.filtered, 'branch', 'fdw_base_18', self.base)
        self.git(self.filtered, 'switch', '-c', 'pg19-upstream', self.old_upstream)
        (self.filtered / 'pg19-only.c').write_text('/* other major must stay unchanged */\n')
        upstream19 = self.commit(self.filtered, 'PostgreSQL 19 upstream')
        self.git(self.filtered, 'merge', '--no-ff', self.patch, '-m', 'Share the same patch with PostgreSQL 19')
        self.base19 = self.git(self.filtered, 'rev-parse', 'HEAD')
        self.assertEqual(set(self.git(self.filtered, 'show', '-s', '--format=%P', self.base19).split()),
                         {upstream19, self.patch})
        self.git(self.filtered, 'branch', 'fdw_base_19', self.base19)
        self.git(self.filtered, 'switch', 'main')
        self.init(self.repo)
        (self.repo / 'README').write_text('unrelated project data\n')
        self.commit(self.repo, 'Project base')
        self.git(self.repo, 'fetch', str(self.filtered), 'fdw_base_18:refs/heads/fdw_base_18',
                 'fdw_base_19:refs/heads/fdw_base_19')
        self.git(self.repo, 'branch', 'upstream/postgres_fdw', self.old_upstream)
        self.git(self.repo, 'subtree', 'add', '--prefix=pgwrh_fdw/18', 'fdw_base_18')
        self.git(self.repo, 'subtree', 'add', '--prefix=pgwrh_fdw/19', 'fdw_base_19')
        self.script = self.repo / 'pgwrh_fdw/tools/import-upstream.py'
        self.script.parent.mkdir(parents=True)
        shutil.copyfile(IMPORTER, self.script)
        self.integrated = self.commit(self.repo, 'Project maintenance tooling')
        (fdw / 'connection.c').write_text('/* new upstream connection fix */\n')
        self.new_source = self.commit(self.pg, 'Upstream fix')
        self.git(self.pg, 'tag', 'REL_18_4')

    def run_import(self, source=None, tag="REL_18_4"):
        return subprocess.run([sys.executable, str(self.script), str(source or self.pg), tag],
                              text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def test_updates_only_upstream_and_reuses_shared_patch_in_subtree(self):
        (self.repo / 'README').write_text('uncommitted project work\n')
        before_status = self.git(self.repo, 'status', '--porcelain')
        result = self.run_import()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD'), self.integrated)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'fdw_base_18'), self.base)
        self.assertEqual(self.git(self.repo, 'status', '--porcelain'), before_status)
        self.assertEqual(self.git(self.repo, 'show', 'HEAD:pgwrh_fdw/18/postgres_fdw.c'),
                         '/* upstream baseline */\n/* local patch */')
        self.assertEqual(self.git(self.repo, 'rev-parse', 'upstream/postgres_fdw^{tree}'),
                         self.git(self.pg, 'rev-parse', 'REL_18_4:contrib/postgres_fdw'))
        self.assertIn(self.new_source, self.git(self.repo, 'cat-file', '-p', 'upstream/REL_18_4'))
        self.git(self.filtered, 'fetch', str(self.repo), 'upstream/postgres_fdw:refs/heads/next-upstream')
        self.git(self.filtered, 'switch', '-c', 'next-fdw', 'next-upstream')
        self.git(self.filtered, 'merge', '--no-ff', self.patch, '-m', 'Merge upstream with the shared patch')
        next_parents = set(self.git(self.filtered, 'show', '-s', '--format=%P', 'next-fdw').split())
        parents19 = set(self.git(self.filtered, 'show', '-s', '--format=%P', self.base19).split())
        self.assertEqual(next_parents & parents19, {self.patch})
        self.assertIn(self.git(self.filtered, 'rev-parse', 'next-upstream'), next_parents)
        self.git(self.repo, 'fetch', str(self.filtered), 'next-fdw:refs/heads/next-fdw')
        self.git(self.repo, 'restore', 'README')
        self.git(self.repo, 'subtree', 'merge', '--prefix=pgwrh_fdw/18', 'next-fdw')
        self.assertTrue((self.repo / 'pgwrh_fdw/18/connection.c').exists())
        self.assertIn('local patch', (self.repo / 'pgwrh_fdw/18/postgres_fdw.c').read_text())
        self.assertTrue(self.script.exists())
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD:pgwrh_fdw/19'),
                         self.git(self.filtered, 'rev-parse', 'fdw_base_19^{tree}'))
        self.assertEqual(self.git(self.repo, 'rev-parse', 'fdw_base_19'), self.base19)
        repeated = self.run_import()
        self.assertNotEqual(repeated.returncode, 0)
        self.assertIn('already exists', repeated.stderr)

    def test_first_pg19_import_preserves_pg18_and_main(self):
        self.git(self.pg, 'tag', 'REL_19_BETA3')
        before = self.git(self.repo, 'rev-parse', 'HEAD', 'upstream/postgres_fdw', 'fdw_base_18', 'fdw_base_19')
        result = self.run_import(tag='REL_19_BETA3')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD', 'upstream/postgres_fdw', 'fdw_base_18', 'fdw_base_19'), before)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'upstream/postgres_fdw_19^{tree}'),
                         self.git(self.pg, 'rev-parse', 'REL_19_BETA3:contrib/postgres_fdw'))
        self.assertIn(self.new_source, self.git(self.repo, 'cat-file', '-p', 'upstream/REL_19_BETA3'))
        self.assertNotEqual(self.run_import(tag='REL_19_BETA3').returncode, 0)

    def test_rejects_unrelated_upstream_without_moving_refs(self):
        other = self.root / 'unrelated-postgres'
        self.init(other)
        fdw = other / 'contrib/postgres_fdw'
        fdw.mkdir(parents=True)
        (fdw / 'postgres_fdw.c').write_text('/* unrelated history */\n')
        self.commit(other, 'Unrelated upstream')
        self.git(other, 'tag', 'REL_18_4')
        before = self.git(self.repo, 'show-ref', '--heads', '--tags')
        result = self.run_import(other)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.git(self.repo, 'show-ref', '--heads', '--tags'), before)


if __name__ == '__main__':
    unittest.main(verbosity=2)

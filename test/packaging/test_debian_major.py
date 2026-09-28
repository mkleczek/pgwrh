"""Check metadata selection before installing dependencies in a build container."""
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class DebianMajorTests(unittest.TestCase):
    def test_pg19_names_paths_and_runtime_dependency(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'debian'
            shutil.copytree(ROOT / 'packaging/deb/debian', target)
            subprocess.run([sys.executable, str(ROOT / 'packaging/deb/select-major.py'),
                            '19', str(target)], check=True)
            control = (target / 'control').read_text()
            self.assertIn('Package: postgresql-19-pgwrh', control)
            self.assertIn('postgresql-server-dev-19', control)
            self.assertIn('postgresql-19-pg-background (>= 2.0.3)', control)
            self.assertNotIn('postgresql-18', control)
            self.assertIn('/postgresql/19/', (target / 'rules').read_text())
            manifest = target / 'postgresql-19-pgwrh.install'
            self.assertTrue(manifest.exists())
            self.assertNotIn('/18/', manifest.read_text())
            self.assertFalse((target / 'postgresql-18-pgwrh.install').exists())

    def test_rejects_unsupported_major_without_changing_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, str(ROOT / 'packaging/deb/select-major.py'),
                                     '20', directory], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(Path(directory).iterdir()), [])

"""Protect package ordering and the boundary between prerelease and stable publication."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'packaging'))
from release_version import release_metadata, validate_release


class ReleaseMetadataTests(unittest.TestCase):
    def test_prerelease_package_versions(self):
        for stage in ('alpha1', 'alpha10', 'beta2', 'rc3'):
            with self.subTest(stage=stage):
                value = release_metadata('1.0.0-' + stage)
                self.assertEqual(value['package_version'], '1.0.0~' + stage)
                self.assertTrue(value['prerelease'])

    def test_final_release(self):
        value = release_metadata('1.0.0')
        self.assertEqual(value['package_version'], '1.0.0')
        self.assertFalse(value['prerelease'])
        validate_release(value, 'v1.0.0', False)

    def test_reject_ambiguous_or_unsafe_versions(self):
        for version in ('1.0', '01.0.0', '1.0.0-alpha', '1.0.0-alpha0',
                        '1.0.0-alpha01', '1.0.0~alpha1', '1.0.0-unknown1',
                        '1.0.0+build1', '1.0.0-alpha1\n', '1.0.0/alpha1'):
            with self.subTest(version=version), self.assertRaises(ValueError):
                release_metadata(version)

    def test_require_matching_release_tag_and_prerelease_status(self):
        value = release_metadata('1.0.0-alpha1')
        validate_release(value, 'v1.0.0-alpha1', True)
        with self.assertRaisesRegex(ValueError, 'tag'):
            validate_release(value, 'v1.0.0', True)
        with self.assertRaisesRegex(ValueError, 'prerelease status'):
            validate_release(value, 'v1.0.0-alpha1', False)
        with self.assertRaisesRegex(ValueError, 'prerelease status'):
            validate_release(release_metadata('1.0.0'), 'v1.0.0', True)

    def test_checked_in_release_metadata_and_workflow_output(self):
        value = release_metadata((ROOT / 'VERSION').read_text().strip())
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'output'
            result = subprocess.run(
                [sys.executable, str(ROOT / 'packaging/check-release.py'),
                 '--tag', 'v' + value['version'], '--github-prerelease',
                 str(value['prerelease']).lower(), '--github-output', str(output)],
                check=True, capture_output=True, text=True)
            self.assertEqual(result.stdout.strip(), value['version'])
            self.assertEqual(dict(line.split('=', 1) for line in output.read_text().splitlines()),
                             {key: str(item).lower() if isinstance(item, bool) else item
                              for key, item in value.items()})


if __name__ == '__main__':
    unittest.main()

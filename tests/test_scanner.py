import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from core.scanner import _collect_volume_space, _format_bytes, _volume_label, extract_video_code, scan_directory


class ScannerExtractionTests(unittest.TestCase):
    def test_standard_code(self):
        self.assertEqual(extract_video_code('ABP-123')[0], 'ABP-123')

    def test_vr_part(self):
        code, part = extract_video_code('VRKM-846-1')
        self.assertEqual(code, 'VRKM-846')
        self.assertEqual(part, 'part1')

    def test_uc_suffix(self):
        self.assertEqual(extract_video_code('ABP-123-UC')[0], 'ABP-123-UC')

    def test_c_suffix(self):
        self.assertEqual(extract_video_code('ABP-123-C')[0], 'ABP-123-C')

    def test_u_suffix(self):
        self.assertEqual(extract_video_code('ABP-123-U')[0], 'ABP-123-U')

    def test_chinese_marker_adds_c_suffix(self):
        self.assertEqual(extract_video_code('ABP-123中文')[0], 'ABP-123-C')

    def test_uncensored_marker_adds_u_suffix(self):
        self.assertEqual(extract_video_code('ABP-123无码')[0], 'ABP-123-U')

    def test_chinese_and_uncensored_markers_add_uc_suffix(self):
        self.assertEqual(extract_video_code('ABP-123中文无码')[0], 'ABP-123-UC')

    def test_unrelated_r_suffix_is_preserved(self):
        self.assertEqual(extract_video_code('ABP-123-R')[0], 'ABP-123-R')

    def test_c_suffix_becomes_part3_when_a_and_b_siblings_exist(self):
        siblings = {'ABP-123-A', 'ABP-123-B', 'ABP-123-C'}
        code, part = extract_video_code('ABP-123-C', siblings)
        self.assertEqual(code, 'ABP-123')
        self.assertEqual(part, 'part3')

    def test_scan_directory_uses_siblings_for_c_part(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ['ABP-123-A.mp4', 'ABP-123-B.mp4', 'ABP-123-C.mp4']:
                (root / name).write_bytes(b'video')

            results, no_code = scan_directory(root)
            by_name = {item['original_name']: item for item in results}

            self.assertEqual(no_code, [])
            self.assertEqual(by_name['ABP-123-C.mp4']['code'], 'ABP-123')
            self.assertEqual(by_name['ABP-123-C.mp4']['part'], 'part3')

    def test_format_bytes(self):
        self.assertEqual(_format_bytes(1024 ** 3), '1.0 GiB')

    def test_volume_label_for_regular_path(self):
        with TemporaryDirectory() as tmp:
            self.assertEqual(_volume_label(Path(tmp)), Path(tmp).anchor)

    def test_collect_volume_space_deduplicates_volume(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            child = root / 'child'
            child.mkdir()
            volumes = _collect_volume_space([str(root), str(child)])
            self.assertEqual(len(volumes), 1)
            self.assertIn('free', volumes[0])


if __name__ == '__main__':
    unittest.main()

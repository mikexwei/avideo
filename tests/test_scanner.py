import unittest

from core.scanner import extract_video_code


class ScannerExtractionTests(unittest.TestCase):
    def test_standard_code(self):
        self.assertEqual(extract_video_code('ABP-123')[0], 'ABP-123')

    def test_vr_part(self):
        code, part = extract_video_code('VRKM-846-1')
        self.assertEqual(code, 'VRKM-846')
        self.assertEqual(part, 'part1')


if __name__ == '__main__':
    unittest.main()

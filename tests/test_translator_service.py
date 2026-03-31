import unittest

from core.translator.service import is_likely_japanese, translate_series_if_japanese


class TranslatorServiceTests(unittest.TestCase):
    def test_is_likely_japanese_detects_kana(self):
        self.assertTrue(is_likely_japanese('美少女シリーズ まじカル'))
        self.assertFalse(is_likely_japanese('Classic Collection'))

    def test_translate_series_skips_non_japanese(self):
        series = 'Classic Collection'
        self.assertEqual(translate_series_if_japanese(series), series)


if __name__ == '__main__':
    unittest.main()

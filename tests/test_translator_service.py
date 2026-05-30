import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.translator.service import is_likely_japanese, select_short_title, shorten_chinese_title, translate_series_if_japanese


class TranslatorServiceTests(unittest.TestCase):
    def test_is_likely_japanese_detects_kana(self):
        self.assertTrue(is_likely_japanese('美少女シリーズ まじカル'))
        self.assertFalse(is_likely_japanese('Classic Collection'))

    def test_translate_series_skips_non_japanese(self):
        series = 'Classic Collection'
        self.assertEqual(translate_series_if_japanese(series), series)

    def test_select_short_title_removes_actor_name(self):
        result = select_short_title(
            ['三上悠亚精美写真三张', '精彩作品'],
            '三上悠亚和新有菜与相泽南精美写真三张',
            ['三上悠亚', '新有菜'],
        )
        self.assertEqual(result, '精美写真三张')

    def test_select_short_title_penalizes_generic_candidates(self):
        result = select_short_title(
            ['精彩作品', '温泉旅行密会'],
            '温泉旅行中的秘密约会',
            [],
        )
        self.assertEqual(result, '温泉旅行密会')

    def test_shorten_chinese_title_selects_best_model_candidate(self):
        class FakeClient:
            def __init__(self, host):
                self.host = host

            def chat(self, model, messages, options):
                return {'message': {'content': '三上悠亚精美写真三张\n精彩作品\n写真特典三张'}}

        fake_ollama = SimpleNamespace(Client=FakeClient)
        with patch.dict('sys.modules', {'ollama': fake_ollama}):
            result = shorten_chinese_title(
                '三上悠亚和新有菜与相泽南精美写真三张',
                actor_names=['三上悠亚', '新有菜'],
            )

        self.assertEqual(result, '精美写真三张')


if __name__ == '__main__':
    unittest.main()

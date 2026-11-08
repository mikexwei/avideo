import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.translator.service import (
    SHORT_TITLE_MODEL,
    is_likely_japanese,
    select_short_title,
    shorten_chinese_title,
    translate_series,
)


class TranslatorServiceTests(unittest.TestCase):
    def test_short_title_uses_gemma_model(self):
        self.assertEqual(SHORT_TITLE_MODEL, 'gemma4:e4b')

    def test_is_likely_japanese_detects_kana(self):
        self.assertTrue(is_likely_japanese('美少女シリーズ まじカル'))
        self.assertFalse(is_likely_japanese('Classic Collection'))

    def test_translate_series_sends_normalized_text_and_returns_simplified_chinese(self):
        with patch('ollama.Client') as client:
            chat = client.return_value.chat
            chat.return_value = {'message': {'content': '時間停止'}}
            self.assertEqual(translate_series(' 時間・停止　！✨ '), '时间停止')
            self.assertTrue(chat.call_args.kwargs['messages'][0]['content'].endswith('時間停止'))

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

    def test_select_short_title_prefers_core_relationship_and_event(self):
        result = select_short_title(
            ['危险欲望', '丈夫眼前被侵犯', '情色刺激'],
            '在丈夫眼前被侵犯危险的欲望',
            [],
        )
        self.assertEqual(result, '丈夫眼前被侵犯')

    def test_select_short_title_rejects_explicit_event_missing_from_source(self):
        result = select_short_title(
            ['被侵犯', '目睹被侵犯', '被他人侵犯'],
            '邦德德',
            [],
        )
        self.assertEqual(result, '')

    def test_short_title_prompt_requires_explicit_events(self):
        captured = {}

        class FakeClient:
            def __init__(self, host):
                pass

            def chat(self, model, messages, options):
                captured['prompt'] = messages[0]['content']
                return {'message': {'content': '丈夫眼前被侵犯\n危险欲望\n情色刺激'}}

        fake_ollama = SimpleNamespace(Client=FakeClient)
        with patch.dict('sys.modules', {'ollama': fake_ollama}):
            result = shorten_chinese_title('在丈夫眼前被侵犯危险的欲望')

        self.assertEqual(result, '丈夫眼前被侵犯')
        self.assertIn('不得弱化美化或审查', captured['prompt'])
        self.assertIn('被未婚夫操到爽了', captured['prompt'])

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

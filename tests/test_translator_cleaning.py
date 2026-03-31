import unittest

from core.translator.cleaning import strip_entities_from_title


class TranslatorCleaningTests(unittest.TestCase):
    def test_strip_actor_and_code(self):
        result = strip_entities_from_title(
            title_jp='ABP-123 桜空もも 完全主観',
            actor_names=['桜空もも'],
            codes=['ABP-123-C'],
        )
        self.assertNotIn('ABP', result)
        self.assertNotIn('桜空', result)


if __name__ == '__main__':
    unittest.main()

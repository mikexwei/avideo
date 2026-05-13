import unittest

from dal.db_manager import _DERIVED_TAGS_TO_REMOVE, _derived_tags_for_video


class DerivedVideoTagsTests(unittest.TestCase):
    def test_leaked_uncensored_tag_wins(self):
        self.assertEqual(
            _derived_tags_for_video('ABP-123-U', 'HEYZO', {'無碼流出'}),
            ['无码流出'],
        )

    def test_u_suffix_adds_cracked_uncensored(self):
        self.assertEqual(
            _derived_tags_for_video('ABP-123-U', '', set()),
            ['无码破解'],
        )

    def test_traditional_cracked_tag_is_normalized(self):
        self.assertEqual(
            _derived_tags_for_video('ABP-123', '', {'無碼破解'}),
            ['无码破解'],
        )

    def test_uc_suffix_adds_uncensored_and_subtitle_tags(self):
        self.assertEqual(
            _derived_tags_for_video('ABP-123-UC', '', set()),
            ['无码破解', '中文字幕'],
        )

    def test_known_maker_adds_original_uncensored(self):
        self.assertEqual(
            _derived_tags_for_video('HEYZO-498', 'HEYZO', set()),
            ['原装无码'],
        )

    def test_c_suffix_adds_subtitle_tag(self):
        self.assertEqual(
            _derived_tags_for_video('ABP-123-C', '', set()),
            ['中文字幕'],
        )

    def test_traditional_leaked_tag_is_removed_during_sync(self):
        self.assertIn('無碼流出', _DERIVED_TAGS_TO_REMOVE)
        self.assertIn('无码流出', _DERIVED_TAGS_TO_REMOVE)
        self.assertIn('無碼破解', _DERIVED_TAGS_TO_REMOVE)
        self.assertIn('无码破解', _DERIVED_TAGS_TO_REMOVE)


if __name__ == '__main__':
    unittest.main()

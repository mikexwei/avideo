import unittest

from utils.fix_db_parts import suffix_only_code


class FixDbPartsTests(unittest.TestCase):
    def test_allows_suffix_change_when_base_matches(self):
        self.assertEqual(suffix_only_code('RBD-800-U', 'RBD-800-UC'), 'RBD-800-UC')

    def test_allows_suffix_removal_when_base_matches(self):
        self.assertEqual(suffix_only_code('ABP-123-C', 'ABP-123'), 'ABP-123')

    def test_preserves_old_code_when_base_changes(self):
        self.assertEqual(suffix_only_code('HEYZO-0498', 'HEYZO-498'), 'HEYZO-0498')

    def test_preserves_old_base_casing(self):
        self.assertEqual(suffix_only_code('abp-123-c', 'ABP-123-UC'), 'abp-123-UC')


if __name__ == '__main__':
    unittest.main()

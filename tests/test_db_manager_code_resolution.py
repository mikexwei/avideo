import sqlite3
import unittest

from dal.db_manager import _resolve_video_code


class DbManagerCodeResolutionTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('CREATE TABLE videos (id INTEGER PRIMARY KEY, code TEXT NOT NULL)')
        self.conn.executemany(
            'INSERT INTO videos (id, code) VALUES (?, ?)',
            [
                (1, 'ABC-001-C'),
                (2, 'DEF-002'),
                (3, 'GHI-003-U'),
                (4, 'GHI-003-UC'),
            ],
        )

    def tearDown(self):
        self.conn.close()

    def test_exact_match_wins(self):
        self.assertEqual(_resolve_video_code(self.conn.cursor(), 'DEF-002'), 'DEF-002')

    def test_base_code_falls_back_to_suffix_variant(self):
        self.assertEqual(_resolve_video_code(self.conn.cursor(), 'ABC-001'), 'ABC-001-C')

    def test_base_fallback_prefers_uc(self):
        self.assertEqual(_resolve_video_code(self.conn.cursor(), 'GHI-003'), 'GHI-003-UC')

    def test_suffix_input_does_not_fall_back_to_other_suffixes(self):
        self.assertIsNone(_resolve_video_code(self.conn.cursor(), 'ABC-001-U'))


if __name__ == '__main__':
    unittest.main()

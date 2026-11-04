import sqlite3
import unittest

from scripts.normalize_tags_simplified import normalize_tags


class NormalizeTagsTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.executescript('''
            CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL);
            CREATE TABLE video_tag_link (
                video_id INTEGER,
                tag_id INTEGER,
                PRIMARY KEY (video_id, tag_id)
            );
            INSERT INTO tags VALUES
                (1, '騎乘位'), (2, '骑乘位'), (3, '女教師'),
                (4, '机翻字幕'), (5, '机翻字幕 V2');
            INSERT INTO video_tag_link VALUES
                (10, 1), (11, 1), (11, 2), (12, 3), (13, 4);
        ''')

    def tearDown(self):
        self.conn.close()

    def test_normalizes_merges_and_deletes_tags(self):
        result = normalize_tags(self.conn)

        tags = self.conn.execute('SELECT id, name FROM tags ORDER BY id').fetchall()
        self.assertEqual(tags, [(2, '骑乘位'), (3, '女教师')])
        links = self.conn.execute(
            'SELECT video_id, tag_id FROM video_tag_link ORDER BY video_id, tag_id'
        ).fetchall()
        self.assertEqual(links, [(10, 2), (11, 2), (12, 3)])
        self.assertEqual(result, {'renamed': 1, 'merged': 1, 'deleted': 2})

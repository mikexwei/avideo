import sqlite3
import unittest
from unittest.mock import patch

from dal.db_manager import get_video_by_code


class VideoDetailPartsTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY, code TEXT, part TEXT, title_jp TEXT, title_zh TEXT,
                release_date TEXT, duration INTEGER, maker TEXT, publisher TEXT, series TEXT,
                score REAL, cover_path TEXT, original_file_path TEXT, scrape_status TEXT,
                file_size INTEGER, file_mtime TEXT, file_birthtime TEXT, deleted INTEGER
            );
            CREATE TABLE actors (id INTEGER PRIMARY KEY, name TEXT, name_zh TEXT, avatar_path TEXT, is_ignored INTEGER DEFAULT 0);
            CREATE TABLE video_actor_link (video_id INTEGER, actor_id INTEGER);
            CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE video_tag_link (video_id INTEGER, tag_id INTEGER);
            CREATE TABLE series_clusters (id INTEGER PRIMARY KEY, canonical_name TEXT, canonical_name_zh TEXT, variations_json TEXT);
            """
        )
        self.conn.executemany(
            """
            INSERT INTO videos (
                id, code, part, original_file_path, scrape_status, file_size,
                file_mtime, file_birthtime, deleted
            ) VALUES (?, 'IPVR-179-U', ?, ?, 'SUCCESS', ?, ?, ?, 0)
            """,
            [
                (1, 'part1', '/videos/IPVR-179-U.part1.mp4', 100, '2026-10-01T10:00:00', '2026-09-01T10:00:00'),
                (2, 'part2', '/videos/IPVR-179-U.part2.mp4', 250, '2026-10-02T10:00:00', '2026-09-02T10:00:00'),
            ],
        )

    def tearDown(self):
        self.conn.close()

    def test_returns_each_part_metadata_and_total_size(self):
        with patch('dal.db_manager._get_conn', return_value=self.conn):
            detail = get_video_by_code('IPVR-179-U')

        self.assertEqual(detail['total_file_size'], 350)
        self.assertEqual(detail['parts'][0]['file_size'], 100)
        self.assertEqual(detail['parts'][0]['file_birthtime'], '2026-09-01T10:00:00')
        self.assertEqual(detail['parts'][1]['file_size'], 250)
        self.assertEqual(detail['parts'][1]['file_mtime'], '2026-10-02T10:00:00')


if __name__ == '__main__':
    unittest.main()

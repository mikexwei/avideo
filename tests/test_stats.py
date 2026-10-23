import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dal.db_manager import get_stats


class StatsTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                title_zh TEXT,
                release_date TEXT,
                maker TEXT,
                score REAL,
                cover_path TEXT,
                scrape_status TEXT,
                file_size INTEGER,
                deleted INTEGER DEFAULT 0
            );
            CREATE TABLE actors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                name_zh TEXT,
                avatar_path TEXT,
                is_ignored INTEGER DEFAULT 0
            );
            CREATE TABLE video_actor_link (
                video_id INTEGER NOT NULL,
                actor_id INTEGER NOT NULL,
                PRIMARY KEY (video_id, actor_id)
            );
            CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE video_tag_link (
                video_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY (video_id, tag_id)
            );
            CREATE TABLE series_clusters (id INTEGER PRIMARY KEY);

            INSERT INTO actors (id, name, name_zh, is_ignored) VALUES
                (1, 'Actor A', '演员A', 0),
                (2, 'Actor B', NULL, 0),
                (3, 'Ignored', NULL, 1);
            INSERT INTO videos
                (id, code, title_zh, release_date, maker, score, cover_path, scrape_status, file_size, deleted)
            VALUES
                (1, 'AAA-001', 'a', '2026-01-01', 'Maker', 4.0, 'a.jpg', 'SUCCESS', 1073741824, 0),
                (2, 'BBB-001', 'b', '2026-01-02', 'Maker', 4.1, 'b.jpg', 'SUCCESS', 2147483648, 0),
                (3, 'CCC-001', 'c', '2026-01-03', 'Maker', 4.2, 'c.jpg', 'SUCCESS', 4294967296, 1),
                (4, 'DDD-001', 'd', '2026-01-04', 'Maker', 4.3, 'd.jpg', 'SUCCESS', 8589934592, 0);
            INSERT INTO video_actor_link (video_id, actor_id) VALUES
                (1, 1),
                (2, 1),
                (2, 2),
                (3, 1),
                (4, 3);
            """
        )
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_actor_disk_usage_sums_non_deleted_non_ignored_videos(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            stats = get_stats()

        usage = stats['actor_disk_usage']

        self.assertEqual([u['name'] for u in usage], ['演员A', 'Actor B'])
        self.assertEqual(usage[0]['size_gb'], 3.0)
        self.assertEqual(usage[0]['video_count'], 2)
        self.assertEqual(usage[1]['size_gb'], 2.0)


if __name__ == '__main__':
    unittest.main()

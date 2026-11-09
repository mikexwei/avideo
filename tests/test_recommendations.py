import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dal.db_manager import get_recommendations
from web.backend.app import app


class RecommendationTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                title_jp TEXT,
                title_zh TEXT,
                release_date TEXT,
                score REAL,
                cover_path TEXT,
                scrape_status TEXT,
                deleted INTEGER DEFAULT 0
            );
            CREATE TABLE tags (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE video_tag_link (
                video_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY (video_id, tag_id)
            );
            INSERT INTO tags (id, name) VALUES
                (1, 'Drama'),
                (2, 'Other');
            INSERT INTO videos
                (id, code, title_jp, title_zh, release_date, score, cover_path, scrape_status, deleted)
            VALUES
                (1, 'LOW-001', 'low', 'low', '2026-01-01', 1.0, 'covers/low.jpg', 'SUCCESS', 0),
                (2, 'DUP-001', 'dup 1', 'dup 1', '2026-01-02', 3.0, 'covers/dup1.jpg', 'SUCCESS', 0),
                (3, 'DUP-001', 'dup 2', 'dup 2', '2026-01-03', 5.0, 'covers/dup2.jpg', 'SUCCESS', 0);
            INSERT INTO video_tag_link (video_id, tag_id) VALUES
                (1, 1),
                (2, 2),
                (3, 2);
            """
        )
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_recommendations_include_low_score_and_dedupe_codes(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            recommendations = get_recommendations(count=8)

        codes = [r['code'] for r in recommendations]

        self.assertIn('LOW-001', codes)
        self.assertEqual(1, codes.count('DUP-001'))
        self.assertEqual(len(codes), len(set(codes)))

    def test_exclude_tags_across_parts_before_pagination_and_recommendation(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                DELETE FROM video_tag_link WHERE video_id=2;
                INSERT INTO videos (id, code, cover_path, scrape_status, deleted) VALUES
                    (4, 'KEEP-001', 'cover.jpg', 'SUCCESS', 0),
                    (5, 'KEEP-001', 'cover.jpg', 'SUCCESS', 1),
                    (6, 'UNTAGGED-001', 'cover.jpg', 'SUCCESS', 0);
                INSERT INTO video_tag_link VALUES (5, 1);
            """)
        query = [('tag', 'Drama'), ('tag', 'Other'), ('exclude', '1')]
        with patch('dal.db_manager.DB_PATH', self.db_path):
            client = app.test_client()
            library = [client.get('/api/videos', query_string=query + [('limit', '1'), ('page', str(page))]).get_json()
                       for page in (1, 2)]
            self.assertEqual([result['total'] for result in library], [2, 2])
            self.assertEqual({result['items'][0]['code'] for result in library}, {'KEEP-001', 'UNTAGGED-001'})
            recommendations = client.get('/api/recommendations', query_string=query).get_json()
            self.assertEqual({item['code'] for item in recommendations}, {'KEEP-001', 'UNTAGGED-001'})
            self.assertEqual(client.get('/api/videos?exclude=1').get_json()['total'], 4)
            included = client.get('/api/videos?tag=Drama').get_json()['items']
            self.assertEqual([item['code'] for item in included], ['LOW-001'])

    def test_tag_catalog_can_include_unused_tags(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO tags VALUES (3, 'Unused')")
            conn.execute("UPDATE videos SET deleted=1 WHERE id=1")
        with patch('dal.db_manager.DB_PATH', self.db_path):
            client = app.test_client()
            active = client.get('/api/tags/all').get_json()
            self.assertEqual({tag['name']: tag['count'] for tag in active}, {'Other': 1})
            catalog = client.get('/api/tags/all?include_unused=1').get_json()
            self.assertEqual({tag['name']: tag['count'] for tag in catalog}, {'Drama': 0, 'Other': 1, 'Unused': 0})


if __name__ == '__main__':
    unittest.main()

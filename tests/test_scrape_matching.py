import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from core.scraper.video_scraper import _pick_matching_index, _search_code_variants, FREEZE
from dal.db_manager import update_video_metadata


class PickMatchingIndexTests(unittest.TestCase):
    def test_exact_match(self):
        self.assertEqual(_pick_matching_index(['ABC-1', 'MFYD-140', 'X-9'], 'MFYD-140'), 1)

    def test_case_insensitive(self):
        self.assertEqual(_pick_matching_index(['mfyd-140'], 'MFYD-140'), 0)

    def test_first_result_wrong_is_skipped(self):
        # First result is a different code — must pick the exact match, not index 0.
        self.assertEqual(_pick_matching_index(['OTHER-999', 'SSIS-500'], 'SSIS-500'), 1)

    def test_no_match_returns_none(self):
        self.assertIsNone(_pick_matching_index(['OTHER-1', 'OTHER-2'], 'MFYD-140'))

    def test_dsvr_matches_3dsvr_result(self):
        self.assertEqual(_pick_matching_index(['OTHER-1', '3DSVR-345'], 'DSVR-345'), 1)

    def test_3dsvr_matches_dsvr_result(self):
        self.assertEqual(_pick_matching_index(['DSVR-345'], '3DSVR-345'), 0)

    def test_empty_list_returns_none(self):
        self.assertIsNone(_pick_matching_index([], 'MFYD-140'))

    def test_empty_search_code_returns_none(self):
        self.assertIsNone(_pick_matching_index(['ABC-1'], ''))

    def test_freeze_sentinel(self):
        self.assertEqual(FREEZE, 'FREEZE')


class SearchCodeVariantsTests(unittest.TestCase):
    def test_dsvr_search_also_tries_3dsvr(self):
        self.assertEqual(_search_code_variants('DSVR-1996'), ['DSVR-1996', '3DSVR-1996'])

    def test_3dsvr_search_also_tries_dsvr(self):
        self.assertEqual(_search_code_variants('3DSVR-1996'), ['3DSVR-1996', 'DSVR-1996'])


class UpdateMetadataFreezeTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                scrape_status TEXT,
                updated_at TIMESTAMP
            );
        """)
        conn.execute("INSERT INTO videos (code, scrape_status) VALUES ('MFYD-140-U', 'PENDING')")
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_freeze_status_is_persisted(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            update_video_metadata(1, 'FREEZE')
        conn = sqlite3.connect(self.db_path)
        status = conn.execute("SELECT scrape_status FROM videos WHERE id=1").fetchone()[0]
        conn.close()
        self.assertEqual(status, 'FREEZE')

    def test_failure_still_maps_to_failed(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            update_video_metadata(1, 'FAILED')
        conn = sqlite3.connect(self.db_path)
        status = conn.execute("SELECT scrape_status FROM videos WHERE id=1").fetchone()[0]
        conn.close()
        self.assertEqual(status, 'FAILED')


class UpdateMetadataActorTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY, code TEXT NOT NULL, title_jp TEXT,
                release_date TEXT, duration TEXT, maker TEXT, publisher TEXT,
                series TEXT, score REAL, cover_path TEXT, scrape_status TEXT,
                file_mtime TEXT, file_birthtime TEXT,
                updated_at TIMESTAMP
            );
            CREATE TABLE actors (
                id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL,
                is_ignored INTEGER DEFAULT 0
            );
            CREATE TABLE video_actor_link (video_id INTEGER, actor_id INTEGER, UNIQUE(video_id, actor_id));
            CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL);
            CREATE TABLE video_tag_link (video_id INTEGER, tag_id INTEGER, UNIQUE(video_id, tag_id));
            INSERT INTO videos (id, code) VALUES (1, 'CAWB-012-UC');
            INSERT INTO actors (id, name, is_ignored) VALUES (1, '旧男优', 1), (2, '清野咲', 0), (3, '野島誠', 1);
            INSERT INTO video_actor_link VALUES (1, 1);
        """)
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_success_replaces_old_links_and_skips_ignored_actors(self):
        data = {
            'title_jp': 'title', 'release_date': '', 'duration': '',
            'maker': '', 'publisher': '', 'series': '', 'score': 0,
            'cover_path': '', 'actors': '清野咲,野島誠', 'categories': '',
        }
        with patch('dal.db_manager.DB_PATH', self.db_path):
            update_video_metadata(1, 'SUCCESS', data)

        conn = sqlite3.connect(self.db_path)
        names = [row[0] for row in conn.execute(
            "SELECT a.name FROM actors a JOIN video_actor_link val ON val.actor_id=a.id WHERE val.video_id=1"
        )]
        conn.close()
        self.assertEqual(names, ['清野咲'])


if __name__ == '__main__':
    unittest.main()

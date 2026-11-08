import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from core.scraper.video_scraper import _pick_matching_index, _search_code_variants, FREEZE
from dal.db_manager import list_all_actors_with_count, patch_actor, patch_video_relations, search_actors, update_video_metadata


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
                updated_at TIMESTAMP, deleted INTEGER DEFAULT 0
            );
            CREATE TABLE actors (
                id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL,
                is_ignored INTEGER DEFAULT 0, avatar_path TEXT, name_zh TEXT
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

    def test_success_converts_scraped_metadata_to_simplified_chinese(self):
        data = {
            'title_jp': '女教師の物語', 'release_date': '', 'duration': '',
            'maker': '亞洲片商', 'publisher': '發行公司', 'series': '戀愛系列',
            'score': 0, 'cover_path': '', 'actors': '橋本有菜',
            'categories': '女教師,騎乘位',
        }
        with patch('dal.db_manager.DB_PATH', self.db_path):
            update_video_metadata(1, 'SUCCESS', data)

        conn = sqlite3.connect(self.db_path)
        video = conn.execute(
            'SELECT title_jp, maker, publisher, series FROM videos WHERE id = 1'
        ).fetchone()
        actors = conn.execute('SELECT name FROM actors ORDER BY name').fetchall()
        tags = conn.execute('SELECT name FROM tags ORDER BY name').fetchall()
        conn.close()
        self.assertEqual(video, ('女教師の物語', '亚洲片商', '发行公司', '戀愛系列'))
        self.assertIn(('桥本有菜',), actors)
        self.assertIn(('女教师',), tags)
        self.assertIn(('骑乘位',), tags)

    def test_actor_variants_reuse_one_record_across_manual_entry_and_scraping(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO actors (id, name) VALUES (10, '橘梨紗')")
        with patch('dal.db_manager.DB_PATH', self.db_path):
            self.assertTrue(patch_video_relations('CAWB-012-UC', actor_names=['橘梨纱', '橘梨紗']))
            self.assertEqual(update_video_metadata(1, 'SUCCESS', {'actors': '橘梨纱,橘梨紗'}), [])
            self.assertEqual([actor['id'] for actor in search_actors('橘梨纱')], [10])
            self.assertEqual([actor['id'] for actor in search_actors('橘梨紗')], [10])
            self.assertEqual(list_all_actors_with_count()[0]['search_names'], ['橘梨纱'])
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT id, name FROM actors WHERE name LIKE '橘梨%'").fetchall(),
                             [(10, '橘梨紗')])
            self.assertEqual(conn.execute("SELECT actor_id FROM video_actor_link WHERE video_id=1").fetchall(), [(10,)])

    def test_actor_rename_merges_normalized_name_and_preserves_profile_and_links(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany("INSERT INTO actors (id,name,avatar_path,name_zh) VALUES (?,?,?,?)", [
                (10, '橘梨纱', 'no_avatar', None),
                (11, '橘梨紗', 'avatars/source.jpg', '译名'),
            ])
            conn.execute("INSERT INTO videos (id,code) VALUES (2,'ABC-002')")
            conn.executemany("INSERT INTO video_actor_link VALUES (?,?)", [(1, 10), (1, 11), (2, 11)])
        with patch('dal.db_manager.DB_PATH', self.db_path):
            self.assertEqual(patch_actor(11, {'name': '橘梨紗'}), 10)
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT avatar_path,name_zh FROM actors WHERE id=10").fetchone(),
                             ('avatars/source.jpg', '译名'))
            self.assertIsNone(conn.execute("SELECT id FROM actors WHERE id=11").fetchone())
            self.assertEqual(conn.execute("SELECT video_id FROM video_actor_link WHERE actor_id=10 ORDER BY video_id").fetchall(),
                             [(1,), (2,)])

    def test_user_can_create_a_new_simplified_tag(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            changed = patch_video_relations('CAWB-012-UC', tag_names=['新標籤'])

        conn = sqlite3.connect(self.db_path)
        tags = conn.execute(
            'SELECT t.name FROM tags t '
            'JOIN video_tag_link l ON l.tag_id = t.id WHERE l.video_id = 1'
        ).fetchall()
        conn.close()
        self.assertTrue(changed)
        self.assertEqual(tags, [('新标签',)])


if __name__ == '__main__':
    unittest.main()

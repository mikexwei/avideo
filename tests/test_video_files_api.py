import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from web.backend.app import app


class VideoFilesEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.mkv = root / 'ABW-123.part1.mkv'
        self.mkv.write_bytes(b'mkv-data')
        self.mp4 = root / 'SSIS-456.mp4'
        self.mp4.write_bytes(b'mp4-data')
        self.missing = root / 'MISSING-001.mkv'

        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                part TEXT,
                original_file_path TEXT,
                file_size INTEGER,
                deleted INTEGER DEFAULT 0
            )
        """)
        conn.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
        conn.execute('CREATE TABLE video_tag_link (video_id INTEGER, tag_id INTEGER)')
        conn.executemany(
            'INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?)',
            [
                (1, 'ABW-123', 'part1', str(self.mkv), 8, 0),
                (2, 'SSIS-456', None, str(self.mp4), 8, 0),
                (3, 'OLD-001', None, str(root / 'OLD-001.mkv'), 8, 1),
                (4, 'MISSING-001', None, str(self.missing), 8, 0),
            ],
        )
        conn.execute("INSERT INTO tags VALUES (1, '中文字幕')")
        conn.execute('INSERT INTO video_tag_link VALUES (1, 1)')
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)
        self.temp_dir.cleanup()

    def test_lists_downloadable_mkv_files_with_download_and_replace_urls(self):
        with patch('web.backend.app.DB_PATH', self.db_path):
            response = self.client.get(
                '/api/videos/files?extension=mkv',
                base_url='http://10.0.0.30:8000',
            )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data['total'], 1)
        self.assertEqual(data['missing'], 1)
        item = data['files'][0]
        self.assertEqual(item['code'], 'ABW-123')
        self.assertEqual(item['part'], 'part1')
        self.assertEqual(item['filename'], self.mkv.name)
        self.assertTrue(item['subtitle_zh'])
        self.assertFalse(item['uncensored_leaked'])
        self.assertFalse(item['uncensored_native'])
        self.assertEqual(
            item['download_url'],
            'http://10.0.0.30:8000/api/videos/ABW-123/stream.mkv?part=part1',
        )
        self.assertEqual(
            item['replace_url'],
            'http://10.0.0.30:8000/api/videos/ABW-123/replace?part=part1',
        )

    def test_defaults_to_mkv_and_rejects_invalid_extension(self):
        with patch('web.backend.app.DB_PATH', self.db_path):
            default_response = self.client.get('/api/videos/files')
            invalid_response = self.client.get('/api/videos/files?extension=../mkv')

        self.assertEqual(default_response.get_json()['extension'], 'mkv')
        self.assertEqual(invalid_response.status_code, 400)


class SubtitleClassificationEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY, code TEXT, part TEXT,
                original_file_path TEXT, file_size INTEGER,
                cover_path TEXT, title_zh TEXT, title_jp TEXT,
                deleted INTEGER DEFAULT 0
            );
            CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
            CREATE TABLE video_tag_link (
                video_id INTEGER, tag_id INTEGER,
                UNIQUE(video_id, tag_id)
            );
            INSERT INTO videos VALUES
                (1, 'IPZ-108-C', 'part1', '/vol/IPZ-108-C.part1.mkv', 100, '', '标题', '', 0),
                (2, 'IPZ-108-C', 'part2', '/vol/IPZ-108-C.part2.mkv', 200, '', '标题', '', 0);
            INSERT INTO tags VALUES (1, '中文字幕');
            INSERT INTO video_tag_link VALUES (1, 1), (2, 1);
        """)
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_lists_candidates_and_classifies_all_parts(self):
        with patch('web.backend.app.DB_PATH', self.db_path):
            listing = self.client.get('/api/videos/subtitle-review')
            response = self.client.post(
                '/api/videos/IPZ-108-C/subtitle-classification',
                json={'classification': '机翻中文'},
            )

        self.assertEqual(listing.status_code, 200)
        self.assertEqual(listing.get_json()[0]['file_count'], 2)
        self.assertEqual(response.status_code, 200)
        conn = sqlite3.connect(self.db_path)
        tags = conn.execute(
            "SELECT v.id, t.name FROM videos v "
            "JOIN video_tag_link l ON l.video_id=v.id "
            "JOIN tags t ON t.id=l.tag_id ORDER BY v.id"
        ).fetchall()
        conn.close()
        self.assertEqual(tags, [(1, '机翻中文'), (2, '机翻中文')])

    def test_rejects_invalid_classification(self):
        with patch('web.backend.app.DB_PATH', self.db_path):
            response = self.client.post(
                '/api/videos/IPZ-108-C/subtitle-classification',
                json={'classification': '其他'},
            )
        self.assertEqual(response.status_code, 400)


if __name__ == '__main__':
    unittest.main()

import io
import os
import queue
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from web.backend.app import app, _Job, _jobs, _jobs_lock


class JobInfraTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_job_dataclass_has_queue_and_status(self):
        j = _Job(job_id='test-123')
        self.assertEqual(j.job_id, 'test-123')
        self.assertIsInstance(j.queue, queue.Queue)
        self.assertEqual(j.status, 'running')

    def test_jobs_dict_is_module_level(self):
        self.assertIsInstance(_jobs, dict)
        self.assertIsInstance(_jobs_lock, type(threading.Lock()))


class MediaLibrariesEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_returns_list_of_strings(self):
        resp = self.client.get('/api/config/media-libraries')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIsInstance(data, list)
        self.assertTrue(all(isinstance(p, str) for p in data))


class MaxCodeEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def _setup_db(self):
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL, part TEXT,
                original_file_path TEXT UNIQUE,
                scrape_status TEXT DEFAULT 'PENDING'
            );
        """)
        conn.execute("INSERT INTO videos (code, original_file_path) VALUES ('WEST-001', '/a/1.mp4')")
        conn.execute("INSERT INTO videos (code, original_file_path) VALUES ('WEST-042', '/a/2.mp4')")
        conn.execute("INSERT INTO videos (code, original_file_path) VALUES ('CN-007', '/a/3.mp4')")
        conn.commit()
        conn.close()
        return path

    def test_returns_max_west(self):
        db = self._setup_db()
        with patch('web.backend.app.DB_PATH', db):
            resp = self.client.get('/api/videos/max-code/WEST')
        os.unlink(db)
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['max_number'], 42)
        self.assertEqual(data['suggested'], 43)

    def test_empty_db_suggests_1(self):
        fd, db = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(db)
        conn.executescript("CREATE TABLE videos (id INTEGER PRIMARY KEY, code TEXT, original_file_path TEXT);")
        conn.commit()
        conn.close()
        with patch('web.backend.app.DB_PATH', db):
            resp = self.client.get('/api/videos/max-code/DUMMY')
        os.unlink(db)
        self.assertEqual(resp.get_json()['suggested'], 1)

    def test_invalid_type_returns_400(self):
        resp = self.client.get('/api/videos/max-code/INVALID')
        self.assertEqual(resp.status_code, 400)


def _init_full_schema(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL, part TEXT,
            original_file_path TEXT UNIQUE NOT NULL,
            title_jp TEXT, title_zh TEXT, title_zh_short TEXT,
            scrape_status TEXT DEFAULT 'PENDING',
            file_size INTEGER, file_mtime TEXT, file_birthtime TEXT,
            deleted INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL);
        CREATE TABLE IF NOT EXISTS video_tag_link (
            video_id INTEGER, tag_id INTEGER, PRIMARY KEY (video_id, tag_id)
        );
        CREATE TABLE IF NOT EXISTS actors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, name_zh TEXT, avatar_path TEXT, is_ignored INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS video_actor_link (
            video_id INTEGER, actor_id INTEGER, PRIMARY KEY (video_id, actor_id)
        );
    """)
    conn.commit()


class AddManualEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.tmpdir = tempfile.mkdtemp()
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        _init_full_schema(conn)
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        os.unlink(self.db_path)

    def _post(self, files=None, volume_path=None, code_type='WEST',
              code_number='1', is_multipart='0'):
        if files is None:
            files = [('files[]', (io.BytesIO(b'video'), 'WEST-001.mp4'))]
        # Group file tuples by key into lists so Werkzeug sends each as a
        # separate multipart field (a list value expands; a tuple does not).
        file_groups: dict = {}
        for key, val in files:
            file_groups.setdefault(key, []).append(val)
        data = {
            'volume_path': volume_path or self.tmpdir,
            'code_type': code_type,
            'code_number': code_number,
            'is_multipart': is_multipart,
            **file_groups,
        }
        with patch('web.backend.app.DB_PATH', self.db_path), \
             patch('dal.db_manager.DB_PATH', self.db_path), \
             patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            return self.client.post('/api/videos/add-manual', data=data,
                                    content_type='multipart/form-data')

    def test_happy_path_creates_db_record(self):
        resp = self._post()
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['codes'], ['WEST-001'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT code, part FROM videos WHERE code='WEST-001'").fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 'WEST-001')

    def test_file_placed_in_code_prefix_subdir(self):
        self._post()
        dest_dir = Path(self.tmpdir) / 'WEST'
        self.assertTrue(dest_dir.exists())
        video_files = list(dest_dir.rglob('*.mp4'))
        self.assertTrue(len(video_files) >= 1)

    def test_multipart_assigns_part_numbers(self):
        files = [
            ('files[]', (io.BytesIO(b'v1'), 'movie_a.mp4')),
            ('files[]', (io.BytesIO(b'v2'), 'movie_b.mp4')),
        ]
        resp = self._post(files=files, is_multipart='1')
        self.assertEqual(resp.status_code, 200)
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT part FROM videos WHERE code='WEST-001' ORDER BY part").fetchall()
        conn.close()
        parts = [r[0] for r in rows]
        self.assertIn('part1', parts)
        self.assertIn('part2', parts)

    def test_invalid_volume_returns_400(self):
        resp = self._post(volume_path='/no/such/volume')
        self.assertEqual(resp.status_code, 400)

    def test_invalid_code_type_returns_400(self):
        resp = self._post(code_type='INVALID')
        self.assertEqual(resp.status_code, 400)

    def test_no_files_returns_400(self):
        with patch('web.backend.app.DB_PATH', self.db_path), \
             patch('dal.db_manager.DB_PATH', self.db_path), \
             patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            resp = self.client.post('/api/videos/add-manual',
                                    data={'volume_path': self.tmpdir, 'code_type': 'WEST',
                                          'code_number': '1', 'is_multipart': '0'},
                                    content_type='multipart/form-data')
        self.assertEqual(resp.status_code, 400)

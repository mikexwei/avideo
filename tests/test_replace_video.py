import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dal.db_manager import replace_video_file


def _init_schema(conn):
    conn.executescript("""
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            part TEXT,
            original_file_path TEXT,
            updated_at TIMESTAMP
        );
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE video_tag_link (
            video_id INTEGER,
            tag_id INTEGER,
            PRIMARY KEY (video_id, tag_id)
        );
    """)
    conn.commit()


class ReplaceVideoFileTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        _init_schema(conn)
        conn.execute("INSERT INTO videos (code, part, original_file_path) VALUES ('SSIS-001', NULL, '/vol/SSIS-001-UC.mp4')")
        conn.execute("INSERT INTO tags (name) VALUES ('中文字幕')")
        conn.execute("INSERT INTO video_tag_link (video_id, tag_id) VALUES (1, 1)")
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def _call(self, code='SSIS-001', part=None, dest='/vol/SSIS-001-C.mp4',
              sub_zh=False, leaked=False, native=False):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            return replace_video_file(code, part, dest, sub_zh, leaked, native)

    def test_updates_file_path(self):
        result = self._call(dest='/vol/SSIS-001-C.mp4', sub_zh=True)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT original_file_path FROM videos WHERE code='SSIS-001'").fetchone()
        conn.close()
        self.assertEqual(row[0], '/vol/SSIS-001-C.mp4')

    def test_adds_missing_tag(self):
        result = self._call(native=True)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        tags = {r[0] for r in conn.execute(
            "SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id=t.id WHERE vtl.video_id=1"
        ).fetchall()}
        conn.close()
        self.assertIn('原装无码', tags)

    def test_removes_tag_when_off(self):
        result = self._call(sub_zh=False)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        tags = {r[0] for r in conn.execute(
            "SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id=t.id WHERE vtl.video_id=1"
        ).fetchall()}
        conn.close()
        self.assertNotIn('中文字幕', tags)

    def test_unknown_code_returns_error(self):
        result = self._call(code='NOSUCH')
        self.assertFalse(result['ok'])


import io
from pathlib import Path

from web.backend.app import app, _compute_final_filename


class ComputeFinalFilenameTests(unittest.TestCase):
    def test_no_toggles(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', False, False, False, None), 'SSIS-001.mp4')

    def test_subtitle_zh(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', True, False, False, None), 'SSIS-001-C.mp4')

    def test_uncensored_leaked(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', False, True, False, None), 'SSIS-001-U.mp4')

    def test_uncensored_native(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', False, False, True, None), 'SSIS-001-U.mp4')

    def test_both_on(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', True, True, False, None), 'SSIS-001-UC.mp4')

    def test_with_part(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', True, False, False, '2'), 'SSIS-001-C.part2.mp4')

    def test_mkv_extension(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mkv', False, False, True, None), 'SSIS-001-U.mkv')


class ReplaceEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_missing_file_returns_400(self):
        resp = self.client.post(
            '/api/videos/SSIS-001/replace',
            data={'file_size': '4', 'dest_name': 'SSIS-001.mp4'},
            content_type='multipart/form-data',
        )
        self.assertEqual(resp.status_code, 400)

    def test_missing_dest_name_returns_400(self):
        resp = self.client.post(
            '/api/videos/SSIS-001/replace',
            data={'file': (io.BytesIO(b'data'), 'upload.mp4'), 'file_size': '4'},
            content_type='multipart/form-data',
        )
        self.assertEqual(resp.status_code, 400)

    def test_video_not_found_returns_404(self):
        with patch('web.backend.app.get_video_file_path', return_value=None):
            resp = self.client.post(
                '/api/videos/NOSUCH/replace',
                data={'file': (io.BytesIO(b'x'), 'u.mp4'), 'file_size': '1', 'dest_name': 'x.mp4'},
                content_type='multipart/form-data',
            )
        self.assertEqual(resp.status_code, 404)

    def test_size_mismatch_deletes_temp_returns_400(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig = Path(tmpdir) / 'SSIS-001.mp4'
            orig.write_bytes(b'original')
            with patch('web.backend.app.get_video_file_path', return_value=str(orig)):
                resp = self.client.post(
                    '/api/videos/SSIS-001/replace',
                    data={'file': (io.BytesIO(b'data'), 'u.mp4'), 'file_size': '9999', 'dest_name': 'SSIS-001.mp4'},
                    content_type='multipart/form-data',
                )
            self.assertEqual(resp.status_code, 400)
            self.assertTrue(orig.exists())
            self.assertFalse(list(Path(tmpdir).glob('*.temp')))

    def test_happy_path_returns_200(self):
        video_bytes = b'new video content'
        with tempfile.TemporaryDirectory() as tmpdir:
            orig = Path(tmpdir) / 'SSIS-001.mp4'
            orig.write_bytes(b'original content')
            with patch('web.backend.app.replace_video_file', return_value={'ok': True}), \
                 patch('web.backend.app.get_video_by_code', return_value={'code': 'SSIS-001', 'tags': [], 'actors': []}), \
                 patch('web.backend.app.get_video_file_path', return_value=str(orig)):
                resp = self.client.post(
                    '/api/videos/SSIS-001/replace',
                    data={
                        'file': (io.BytesIO(video_bytes), 'upload.mp4'),
                        'file_size': str(len(video_bytes)),
                        'dest_name': 'SSIS-001.mp4',
                        'subtitle_zh': '1',
                    },
                    content_type='multipart/form-data',
                )
            self.assertEqual(resp.status_code, 200)
            self.assertFalse(orig.exists())
            self.assertTrue((Path(tmpdir) / 'SSIS-001-C.mp4').exists())
            self.assertEqual((Path(tmpdir) / 'SSIS-001-C.mp4').read_bytes(), video_bytes)

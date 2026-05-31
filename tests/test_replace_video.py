import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


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
            from dal.db_manager import replace_video_file
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

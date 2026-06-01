import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from web.backend.app import app


def _make_db(rows=None):
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            part TEXT,
            original_file_path TEXT UNIQUE
        );
    """)
    for row in (rows or []):
        conn.execute(
            'INSERT INTO videos (code, part, original_file_path) VALUES (?, ?, ?)',
            row,
        )
    conn.commit()
    conn.close()
    return path


class BatchCheckEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def _post(self, filenames, db):
        with patch('web.backend.app.DB_PATH', db):
            return self.client.post(
                '/api/videos/batch-check',
                data=json.dumps({'filenames': filenames}),
                content_type='application/json',
            )

    def test_existing_code_returns_exists_true(self):
        db = _make_db([('ABW-123-C', None, '/vol/ABW-123-C/ABW-123-C.mp4')])
        resp = self._post(['ABW-123-C.mp4'], db)
        os.unlink(db)
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(len(data), 1)
        self.assertTrue(data[0]['exists'])
        self.assertEqual(data[0]['code'], 'ABW-123-C')
        self.assertIsNone(data[0]['part'])
        self.assertEqual(data[0]['original_path'], '/vol/ABW-123-C/ABW-123-C.mp4')

    def test_non_existing_code_returns_exists_false(self):
        db = _make_db()
        resp = self._post(['SSIS-999.mkv'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertFalse(data[0]['exists'])
        self.assertEqual(data[0]['code'], 'SSIS-999')
        self.assertIsNone(data[0]['original_path'])
        self.assertEqual(data[0]['db_parts'], [])

    def test_part_extracted_from_filename(self):
        db = _make_db()
        resp = self._post(['WEST-042-part1.mp4'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertEqual(data[0]['code'], 'WEST-042')
        self.assertEqual(data[0]['part'], 'part1')
        self.assertFalse(data[0]['exists'])

    def test_db_parts_returned_for_multipart_video(self):
        db = _make_db([
            ('WEST-010', 'part1', '/vol/WEST-010/a.mp4'),
            ('WEST-010', 'part2', '/vol/WEST-010/b.mp4'),
        ])
        resp = self._post(['WEST-010.mkv'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertTrue(data[0]['exists'])
        self.assertIn('part1', data[0]['db_parts'])
        self.assertIn('part2', data[0]['db_parts'])

    def test_unparseable_filename_returns_null_exists_and_error(self):
        db = _make_db()
        resp = self._post(['random_file_no_code.mp4'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertIsNone(data[0]['exists'])
        self.assertIsNone(data[0]['code'])
        self.assertIn('error', data[0])

    def test_empty_list_returns_empty_array(self):
        db = _make_db()
        resp = self._post([], db)
        os.unlink(db)
        self.assertEqual(resp.get_json(), [])

    def test_multiple_filenames_processed_correctly(self):
        db = _make_db([('ABW-123', None, '/vol/ABW-123/ABW-123.mp4')])
        resp = self._post(['ABW-123.mp4', 'NEW-999.mp4'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertEqual(len(data), 2)
        self.assertTrue(data[0]['exists'])
        self.assertFalse(data[1]['exists'])

    def test_non_list_filenames_returns_400(self):
        resp = self.client.post(
            '/api/videos/batch-check',
            data=json.dumps({'filenames': 'not-a-list'}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 400)

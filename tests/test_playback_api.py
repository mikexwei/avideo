import sqlite3
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from tests.test_add_video_api import _init_full_schema
from web.backend.app import app


class PlaybackApiTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'test.db'
        with sqlite3.connect(self.path) as conn:
            _init_full_schema(conn)
            for field in ('release_date', 'score', 'cover_path'):
                conn.execute(f'ALTER TABLE videos ADD COLUMN {field} TEXT')
            conn.executemany('INSERT INTO videos(code,part,original_file_path) VALUES (?,?,?)', [
                ('DEMO-100-UC', 'part1', '/tmp/first.mkv'), ('DEMO-100-UC', 'part2', '/tmp/second.mkv'),
                ('DEMO-200', None, '/tmp/third.mp4'), ('DEMO-300', None, '/tmp/fourth.mp4'),
            ])
        for target in ('web.backend.app.DB_PATH', 'dal.db_manager.DB_PATH'):
            guard = patch(target, self.path)
            guard.start()
            self.addCleanup(guard.stop)
        self.client = app.test_client()

    def test_reports_are_idempotent_and_sorts_apply_before_pagination(self):
        endpoint = '/api/videos/DEMO-100/playback'
        session = endpoint + '/' + str(uuid.uuid4())
        recent = int(time.time() * 1000) - 500
        data = dict(started_at=recent - 30_000, last_played_at=recent - 1000, watched_ms=5000,
                    position_ms=18000, sequence=2, part='part2')
        for report in (data, data, {**data, 'sequence': 1, 'position_ms': 0}):
            self.assertEqual(self.client.post(session, json=report).status_code, 204)
        history = self.client.get(endpoint).get_json()
        self.assertEqual(history['play_count'], 1)
        self.assertEqual(history['watched_ms'], 5000)
        self.assertEqual(history['sessions'][0]['position_ms'], 18000)
        self.assertEqual(history['sessions'][0]['part'], 'part2')

        response = self.client.post('/api/videos/DEMO-200/playback/' + str(uuid.uuid4()),
                                    data={**data, 'part': '', 'source': 'iina', 'watched_ms': 1000, 'last_played_at': recent})
        self.assertEqual(response.status_code, 204)
        for sort, order in [('last_played', ['DEMO-200','DEMO-100-UC','DEMO-300']),
                            ('watched', ['DEMO-100-UC','DEMO-200','DEMO-300'])]:
            for page, expected in enumerate(order, 1):
                data = self.client.get(f'/api/videos?sort={sort}&page={page}&limit=1').get_json()
                self.assertEqual(data['total'], 3)
                self.assertEqual(data['items'][0]['code'], expected)

    def test_direct_click_counts_once_without_adding_watch_time(self):
        endpoint = '/api/videos/DEMO-100/playback'
        session = endpoint + '/' + str(uuid.uuid4())
        timestamp = int(time.time() * 1000)
        data = dict(source='direct', part='part2', started_at=timestamp,
                    last_played_at=timestamp, watched_ms=0, position_ms=0, sequence=1)
        for _ in range(2):
            self.assertEqual(self.client.post(session, json=data).status_code, 204)
        self.assertEqual(self.client.post(session, json={**data, 'watched_ms': 100}).status_code, 400)
        self.assertEqual(self.client.post(session, json={**data, 'source': 'web'}).status_code, 400)
        history = self.client.get(endpoint).get_json()
        self.assertEqual((history['play_count'], history['watched_ms'], history['last_played_at']), (1, 0, timestamp))
        self.assertEqual((history['sessions'][0]['source'], history['sessions'][0]['part']), ('direct', 'part2'))

    def test_file_reads_do_not_create_a_session(self):
        path = self.path.with_suffix('.mkv')
        path.write_bytes(b'file')
        with patch('web.backend.app.get_video_file_path', return_value=str(path)):
            response = self.client.get('/api/videos/DEMO-100/stream.mkv?download=1')
            self.assertEqual(response.status_code, 200)
            response.close()
            response = self.client.get('/api/videos/DEMO-100/stream.mkv')
            self.assertEqual(response.status_code, 200)
            response.close()
        self.assertEqual(self.client.get('/api/videos/DEMO-100/playback').get_json()['play_count'], 0)

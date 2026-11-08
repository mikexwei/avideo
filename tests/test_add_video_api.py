import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from web.backend.app import app


_queue_mock = MagicMock()
_queue_mock.get.return_value = None
_queue_mock.connect.return_value.__enter__.return_value.execute.return_value.rowcount = 1


def setUpModule():
    global _queue_patch, _worker_patch
    _queue_patch = patch('web.backend.app.scrape_store', return_value=_queue_mock)
    _worker_patch = patch('web.backend.app.ensure_worker')
    _queue_patch.start()
    _worker_patch.start()
    global _prepare_patch
    _prepare_patch = patch('web.backend.app.prepare_upload', return_value='SSIS-001')
    _prepare_patch.start()


def tearDownModule():
    _queue_patch.stop()
    _worker_patch.stop()
    _prepare_patch.stop()


class MediaLibrariesEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_returns_list_of_objects_with_path_and_free_gb(self):
        resp = self.client.get('/api/config/media-libraries')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIsInstance(data, list)
        for item in data:
            self.assertIn('path', item)
            self.assertIsInstance(item['path'], str)
            self.assertIn('free_gb', item)

    def test_returns_disk_usage_fields_for_available_library(self):
        tmpdir = tempfile.mkdtemp()
        try:
            with patch('web.backend.app.MEDIA_LIBRARIES', [Path(tmpdir)]):
                resp = self.client.get('/api/config/media-libraries')
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(len(data), 1)
        self.assertTrue(data[0]['available'])
        self.assertEqual(data[0]['path'], tmpdir)
        self.assertIn('name', data[0])
        self.assertIn('used_gb', data[0])
        self.assertIn('total_gb', data[0])
        self.assertIn('used_percent', data[0])
        self.assertIn('free_percent', data[0])

    def test_disk_usage_uses_df_values(self):
        df_output = (
            'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
            '/dev/disk2s2 11718482176 11554084864 164397312 99% /Volumes/12TB\n'
        )
        with patch('web.backend.app.MEDIA_LIBRARIES', [Path('/Volumes/12TB/avideo')]), \
             patch('subprocess.check_output', return_value=df_output):
            resp = self.client.get('/api/config/media-libraries')

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data[0]['total_gb'], 11175.6)
        self.assertEqual(data[0]['used_gb'], 11018.8)
        self.assertEqual(data[0]['free_gb'], 156.8)
        self.assertEqual(data[0]['used_percent'], 98.6)
        self.assertEqual(data[0]['free_percent'], 1.4)


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

    def test_scrape_status_is_success(self):
        # Manual uploads carry user-provided metadata, so they should not be PENDING.
        self._post()
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT scrape_status FROM videos WHERE code='WEST-001'").fetchone()
        conn.close()
        self.assertEqual(row[0], 'SUCCESS')

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


class StreamingUploadTests(unittest.TestCase):
    """Per-part streaming upload (upload-part) + upload_id finalize path.

    This is the path that fixes large multi-part batches: each file is streamed
    in its own request (under the 50GB cap), then add-manual/add-auto reads them
    from the tmp dir via upload_id instead of one giant multipart request.
    """

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

    def _upload_part(self, upload_id, name, body):
        with patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            return self.client.post(
                '/api/videos/upload-part',
                data=body,
                headers={
                    'X-Upload-Id': upload_id,
                    'X-Volume-Path': self.tmpdir,
                    'X-Dest-Name': name,
                    'X-File-Size': str(len(body)),
                },
                content_type='application/octet-stream',
            )

    def test_upload_part_writes_file_to_tmp_dir(self):
        uid = '11111111-1111-4111-8111-111111111111'
        resp = self._upload_part(uid, 'WEST-001-part1.mp4', b'aaaa')
        self.assertEqual(resp.status_code, 200)
        written = Path(self.tmpdir) / '.upload_tmp' / uid / 'WEST-001-part1.mp4'
        self.assertTrue(written.exists())
        self.assertEqual(written.read_bytes(), b'aaaa')

    def test_upload_part_rejects_bad_upload_id(self):
        resp = self._upload_part('../escape', 'x.mp4', b'a')
        self.assertEqual(resp.status_code, 400)

    def test_upload_part_size_mismatch_returns_400(self):
        uid = '22222222-2222-4222-8222-222222222222'
        with patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            resp = self.client.post(
                '/api/videos/upload-part', data=b'aaaa',
                headers={'X-Upload-Id': uid, 'X-Volume-Path': self.tmpdir,
                         'X-Dest-Name': 'x.mp4', 'X-File-Size': '999'},
                content_type='application/octet-stream',
            )
        self.assertEqual(resp.status_code, 400)

    def test_upload_part_canonicalizes_ad_prefix_before_rescan(self):
        uid = '77777777-7777-4777-8777-777777777777'
        resp = self._upload_part(uid, r'489155.com\@MIAB-679-UC.mp4', b'video')

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['name'], 'MIAB-679-UC.mp4')
        written = Path(self.tmpdir) / '.upload_tmp' / uid / 'MIAB-679-UC.mp4'
        self.assertEqual(written.read_bytes(), b'video')

    def test_add_manual_via_upload_id_creates_multipart_record(self):
        uid = '33333333-3333-4333-8333-333333333333'
        self.assertEqual(self._upload_part(uid, 'movie_a.mp4', b'v1').status_code, 200)
        self.assertEqual(self._upload_part(uid, 'movie_b.mp4', b'v2').status_code, 200)
        with patch('web.backend.app.DB_PATH', self.db_path), \
             patch('dal.db_manager.DB_PATH', self.db_path), \
             patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            resp = self.client.post('/api/videos/add-manual', json={
                'upload_id': uid, 'volume_path': self.tmpdir,
                'code_type': 'WEST', 'code_number': 1, 'is_multipart': True,
            })
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()['codes'], ['WEST-001'])
        conn = sqlite3.connect(self.db_path)
        parts = [r[0] for r in conn.execute(
            "SELECT part FROM videos WHERE code='WEST-001' ORDER BY part").fetchall()]
        conn.close()
        self.assertIn('part1', parts)
        self.assertIn('part2', parts)

    def test_add_auto_via_upload_id_returns_job_id(self):
        uid = '44444444-4444-4444-8444-444444444444'
        self.assertEqual(self._upload_part(uid, 'SSIS-001.mp4', b'v').status_code, 200)
        with patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]), \
             patch('web.backend.app.prepare_upload', return_value='SSIS-001'):
            resp = self.client.post('/api/videos/add-auto', json={
                'upload_id': uid, 'volume_path': self.tmpdir,
            })
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.get_json()['job_id'], uid)

    def test_add_auto_via_upload_id_no_files_returns_400(self):
        uid = '55555555-5555-4555-8555-555555555555'
        with patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            resp = self.client.post('/api/videos/add-auto', json={
                'upload_id': uid, 'volume_path': self.tmpdir,
            })
        self.assertEqual(resp.status_code, 400)

    def test_macos_appledouble_sidecars_are_ignored(self):
        """exFAT volumes drop a 4KB ._<name> sidecar next to every write; it
        must never be treated as a video part (regression: bogus 4KB part1)."""
        uid = '66666666-6666-4666-8666-666666666666'
        tmp_dir = Path(self.tmpdir) / '.upload_tmp' / uid
        tmp_dir.mkdir(parents=True)
        (tmp_dir / 'WEST-001.mp4').write_bytes(b'real video bytes')
        (tmp_dir / '._WEST-001.mp4').write_bytes(b'\x00' * 4096)  # AppleDouble
        (tmp_dir / '.DS_Store').write_bytes(b'\x00' * 4096)
        with patch('web.backend.app.DB_PATH', self.db_path), \
             patch('dal.db_manager.DB_PATH', self.db_path), \
             patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            resp = self.client.post('/api/videos/add-manual', json={
                'upload_id': uid, 'volume_path': self.tmpdir,
                'code_type': 'WEST', 'code_number': 1, 'is_multipart': True,
            })
        self.assertEqual(resp.status_code, 200)
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            "SELECT part, file_size FROM videos WHERE code='WEST-001'").fetchall()
        conn.close()
        # Exactly one record (the real file), no bogus 4KB sidecar part.
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], len(b'real video bytes'))


class FinalizeRenameEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.tmpdir = tempfile.mkdtemp()
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        _init_full_schema(conn)
        # Create a real file
        self.video_file = Path(self.tmpdir) / 'WEST-001.mp4'
        self.video_file.write_bytes(b'fake video')
        conn.execute(
            "INSERT INTO videos (code, part, original_file_path, title_zh_short, title_zh, title_jp) "
            "VALUES ('WEST-001', NULL, ?, '短标题', '中文标题', '日文標題')",
            (str(self.video_file),)
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        os.unlink(self.db_path)

    def _call(self, code='WEST-001'):
        with patch('web.backend.app.DB_PATH', self.db_path):
            return self.client.post(f'/api/videos/{code}/finalize-rename')

    def test_renames_file_to_build_filename_result(self):
        resp = self._call()
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data['ok'])
        new_path = Path(data['new_paths'][0])
        self.assertTrue(new_path.exists())
        # build_filename with no actors gives: WEST-001.[短标题].mp4
        self.assertIn('WEST-001', new_path.name)
        self.assertIn('短标题', new_path.name)

    def test_updates_db_original_file_path(self):
        self._call()
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT original_file_path FROM videos WHERE code='WEST-001'").fetchone()
        conn.close()
        self.assertNotEqual(row[0], str(self.video_file))
        self.assertIn('短标题', row[0])

    def test_moves_file_to_code_prefix_directory_under_avideo_root(self):
        root = Path(self.tmpdir) / 'avideo'
        wrong_dir = root / 'WRONG'
        wrong_dir.mkdir(parents=True)
        old_path = wrong_dir / 'WEST-001.mp4'
        old_path.write_bytes(b'fake video')
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "UPDATE videos SET original_file_path = ? WHERE code = 'WEST-001'",
            (str(old_path),),
        )
        conn.commit()
        conn.close()

        resp = self._call()
        self.assertEqual(resp.status_code, 200)
        new_path = Path(resp.get_json()['new_paths'][0])
        self.assertEqual(new_path.parent, root / 'WEST')
        self.assertTrue(new_path.exists())
        self.assertFalse(old_path.exists())

    def test_missing_code_returns_404(self):
        resp = self._call(code='NOSUCH')
        self.assertEqual(resp.status_code, 404)

    def test_skips_rename_when_name_unchanged(self):
        """If build_filename produces the same name, no error."""
        conn = sqlite3.connect(self.db_path)
        # Remove short title so build_filename gives bare code name
        conn.execute("UPDATE videos SET title_zh_short=NULL, title_zh=NULL, title_jp=NULL WHERE code='WEST-001'")
        conn.commit()
        conn.close()
        # Rename the actual file to what build_filename would produce (WEST-001.mp4)
        target = self.video_file.parent / 'WEST-001.mp4'
        if not target.exists():
            self.video_file.rename(target)
        conn2 = sqlite3.connect(self.db_path)
        conn2.execute("UPDATE videos SET original_file_path=? WHERE code='WEST-001'", (str(target),))
        conn2.commit()
        conn2.close()
        with patch('web.backend.app.DB_PATH', self.db_path):
            resp = self.client.post('/api/videos/WEST-001/finalize-rename')
        self.assertEqual(resp.status_code, 200)


class AddAutoEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_returns_job_id(self):
        with patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]), \
             patch('web.backend.app.prepare_upload', return_value='SSIS-001'):
            resp = self.client.post(
                '/api/videos/add-auto',
                data={
                    'volume_path': self.tmpdir,
                    'files[]': (io.BytesIO(b'video'), 'SSIS-001.mp4'),
                },
                content_type='multipart/form-data',
            )
        self.assertEqual(resp.status_code, 202)
        data = resp.get_json()
        self.assertIn('job_id', data)
        self.assertIsInstance(data['job_id'], str)

    def test_invalid_volume_returns_400(self):
        resp = self.client.post(
            '/api/videos/add-auto',
            data={'volume_path': '/no/such/vol', 'files[]': (io.BytesIO(b'v'), 'x.mp4')},
            content_type='multipart/form-data',
        )
        self.assertEqual(resp.status_code, 400)

    def test_no_files_returns_400(self):
        with patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
            resp = self.client.post(
                '/api/videos/add-auto',
                data={'volume_path': self.tmpdir},
                content_type='multipart/form-data',
            )
        self.assertEqual(resp.status_code, 400)

    def test_accepts_any_existing_tags_and_custom_subtitle_classification(self):
        db_fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(db_fd)
        conn = sqlite3.connect(db_path)
        conn.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT UNIQUE)')
        conn.executemany(
            'INSERT INTO tags(name) VALUES (?)',
            [('机翻中文 V2',), ('VR',)],
        )
        conn.commit()
        conn.close()
        try:
            with patch('web.backend.app.DB_PATH', Path(db_path)), \
                 patch('web.backend.app.MEDIA_LIBRARIES', [Path(self.tmpdir)]):
                resp = self.client.post(
                    '/api/videos/add-auto',
                    data={
                        'volume_path': self.tmpdir,
                        'files[]': (io.BytesIO(b'video'), 'SSIS-001.mp4'),
                        'subtitle_classification': '机翻中文 V2',
                        'tags': json.dumps(['VR']),
                    },
                    content_type='multipart/form-data',
                )

            self.assertEqual(resp.status_code, 202)
            payload = json.loads(_queue_mock.connect.return_value.__enter__.return_value.execute.call_args.args[1][1])
            self.assertEqual(payload['subtitle_classification'], '机翻中文 V2')
            self.assertEqual(payload['additional_tags'], ['VR'])
        finally:
            os.unlink(db_path)

    def test_rejects_unknown_upload_tags(self):
        db_fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(db_fd)
        conn = sqlite3.connect(db_path)
        conn.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT UNIQUE)')
        conn.commit()
        conn.close()
        try:
            with patch('web.backend.app.DB_PATH', Path(db_path)):
                resp = self.client.post(
                    '/api/videos/add-auto',
                    json={'tags': ['不存在的标签']},
                )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()['tags'], ['不存在的标签'])
        finally:
            os.unlink(db_path)


class RescrapeEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL, part TEXT,
                original_file_path TEXT UNIQUE,
                scrape_status TEXT DEFAULT 'PENDING',
                deleted INTEGER DEFAULT 0
            );
        """)
        conn.execute("INSERT INTO videos (code, original_file_path, scrape_status) VALUES ('ABW-1', '/a/1.mp4', 'FAILED')")
        conn.execute("INSERT INTO videos (code, original_file_path, scrape_status) VALUES ('ABW-2', '/a/2.mp4', 'FAILED')")
        conn.execute("INSERT INTO videos (code, original_file_path, scrape_status) VALUES ('ABW-3', '/a/3.mp4', 'SUCCESS')")
        conn.execute("INSERT INTO videos (code, original_file_path, scrape_status, deleted) VALUES ('ABW-4', '/a/4.mp4', 'FAILED', 1)")
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_invalid_status_returns_400(self):
        resp = self.client.post('/api/videos/rescrape',
                                data=json.dumps({'scrape_status': 'SUCCESS'}),
                                content_type='application/json')
        self.assertEqual(resp.status_code, 400)

    def test_returns_distinct_matching_codes_and_job_id(self):
        with patch('web.backend.app.DB_PATH', self.db_path), \
             patch('web.backend.app.ensure_worker'):
            resp = self.client.post('/api/videos/rescrape',
                                    data=json.dumps({'scrape_status': 'FAILED'}),
                                    content_type='application/json')
        self.assertEqual(resp.status_code, 202)
        data = resp.get_json()
        self.assertIn('job_id', data)
        # ABW-1, ABW-2 match; ABW-3 is SUCCESS, ABW-4 is deleted
        self.assertEqual(sorted(data['codes']), ['ABW-1', 'ABW-2'])

    def test_no_matching_videos_returns_400(self):
        with patch('web.backend.app.DB_PATH', self.db_path):
            resp = self.client.post('/api/videos/rescrape',
                                    data=json.dumps({'scrape_status': 'PENDING'}),
                                    content_type='application/json')
        self.assertEqual(resp.status_code, 400)

    def test_single_video_rescrape_returns_job_id(self):
        with patch('web.backend.app.DB_PATH', self.db_path), \
             patch('web.backend.app.ensure_worker'):
            resp = self.client.post('/api/videos/ABW-1/rescrape')
        self.assertEqual(resp.status_code, 202)
        data = resp.get_json()
        self.assertIn('job_id', data)
        self.assertEqual(data['code'], 'ABW-1')

    def test_single_video_rescrape_missing_returns_404(self):
        with patch('web.backend.app.DB_PATH', self.db_path):
            resp = self.client.post('/api/videos/NOSUCH/rescrape')
        self.assertEqual(resp.status_code, 404)

    def test_single_video_rescrape_deleted_returns_404(self):
        with patch('web.backend.app.DB_PATH', self.db_path):
            resp = self.client.post('/api/videos/ABW-4/rescrape')
        self.assertEqual(resp.status_code, 404)


class JobStreamEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_unknown_job_returns_404(self):
        resp = self.client.get('/api/jobs/nonexistent-id/stream')
        self.assertEqual(resp.status_code, 404)

    def test_streams_events_from_durable_job(self):
        from core.browser_queue import RuntimeStore
        with tempfile.TemporaryDirectory() as directory:
            store = RuntimeStore(Path(directory) / 'queue.db')
            task_id = store.submit('login_status')
            store.event(task_id, {'step': 'done', 'status': 'done', 'message': 'complete'})
            with patch('web.backend.app.scrape_store', return_value=store):
                response = self.client.get('/api/jobs/' + task_id + '/stream')
                self.assertEqual(response.status_code, 200)
                self.assertIn('"step": "done"', response.get_data(as_text=True))


if __name__ == '__main__':
    unittest.main()

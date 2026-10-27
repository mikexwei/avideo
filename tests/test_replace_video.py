import io
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dal.db_manager import replace_multipart_with_single_file, replace_video_file


def _init_schema(conn):
    conn.executescript("""
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            part TEXT,
            original_file_path TEXT,
            file_size INTEGER,
            file_mtime TEXT,
            file_birthtime TEXT,
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
        CREATE TABLE video_actor_link (
            video_id INTEGER,
            actor_id INTEGER,
            PRIMARY KEY (video_id, actor_id)
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
              sub_zh=False, leaked=False, native=False, file_meta=None):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            return replace_video_file(code, part, dest, sub_zh, leaked, native, file_meta=file_meta)

    def test_updates_file_path(self):
        result = self._call(dest='/vol/SSIS-001-C.mp4', sub_zh=True)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT original_file_path FROM videos WHERE id=1").fetchone()
        conn.close()
        self.assertEqual(row[0], '/vol/SSIS-001-C.mp4')

    def test_syncs_code_suffix(self):
        # -C record replaced by a -UC file: code must follow the new suffix.
        result = self._call(dest='/vol/SSIS-001-UC.mp4', sub_zh=True, leaked=True)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT code FROM videos WHERE id=1").fetchone()
        conn.close()
        self.assertEqual(row[0], 'SSIS-001-UC')

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

    def test_updates_file_stats(self):
        meta = {'file_size': 12345, 'file_mtime': '2026-07-03T10:00:00', 'file_birthtime': '2026-07-03T09:00:00'}
        result = self._call(sub_zh=True, file_meta=meta)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT file_size, file_mtime, file_birthtime FROM videos WHERE id=1"
        ).fetchone()
        conn.close()
        self.assertEqual(tuple(row), (12345, '2026-07-03T10:00:00', '2026-07-03T09:00:00'))

    def test_cracked_tag_uses_replacement_birthtime(self):
        meta = {'file_size': 1, 'file_mtime': '2026-10-11T10:00:00', 'file_birthtime': '2026-10-11T09:00:00'}
        result = self._call(leaked=True, file_meta=meta)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        tags = {r[0] for r in conn.execute(
            "SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id=t.id WHERE vtl.video_id=1"
        )}
        conn.close()
        self.assertIn('LTX 无码破解', tags)
        self.assertNotIn('无码破解', tags)

    def test_replace_multipart_with_single_file_collapses_rows(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM video_tag_link")
        conn.execute("DELETE FROM videos")
        conn.execute("INSERT INTO videos (id, code, part, original_file_path) VALUES (1, 'ABW-123', 'part1', '/vol/ABW-123.part1.mp4')")
        conn.execute("INSERT INTO videos (id, code, part, original_file_path) VALUES (2, 'ABW-123', 'part2', '/vol/ABW-123.part2.mp4')")
        conn.execute("INSERT INTO videos (id, code, part, original_file_path) VALUES (3, 'ABW-123', 'part3', '/vol/ABW-123.part3.mp4')")
        conn.execute("INSERT OR IGNORE INTO tags (name) VALUES ('中文字幕')")
        tag_id = conn.execute("SELECT id FROM tags WHERE name = '中文字幕'").fetchone()[0]
        for video_id in (1, 2, 3):
            conn.execute("INSERT INTO video_tag_link (video_id, tag_id) VALUES (?, ?)", (video_id, tag_id))
        conn.commit()
        conn.close()

        with patch('dal.db_manager.DB_PATH', self.db_path):
            result = replace_multipart_with_single_file(
                'ABW-123', '/vol/ABW-123-C.mp4', True, False, False,
                file_meta={'file_size': 9, 'file_mtime': 'm', 'file_birthtime': 'b'},
                keep_part='part2',
            )

        self.assertTrue(result['ok'])
        self.assertEqual(result['new_code'], 'ABW-123-C')
        self.assertEqual(
            result['old_paths'],
            ['/vol/ABW-123.part1.mp4', '/vol/ABW-123.part2.mp4', '/vol/ABW-123.part3.mp4'],
        )
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT id, code, part, original_file_path, file_size FROM videos ORDER BY id").fetchall()
        links = conn.execute("SELECT video_id FROM video_tag_link ORDER BY video_id").fetchall()
        conn.close()
        self.assertEqual(rows, [(2, 'ABW-123-C', None, '/vol/ABW-123-C.mp4', 9)])
        self.assertEqual(links, [(2,)])


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

    def test_suffix_goes_on_code_not_end(self):
        # Canonical name: code.actor.[title].ext — suffix belongs on the code component.
        self.assertEqual(
            _compute_final_filename('ABW-123.女优.[标题].mp4', True, False, False, None),
            'ABW-123-C.女优.[标题].mp4',
        )

    def test_existing_suffix_on_code_is_swapped(self):
        # Toggling from -C to uncensored should replace the code suffix, not stack it.
        self.assertEqual(
            _compute_final_filename('ABW-123-C.女优.[标题].mp4', False, True, False, None),
            'ABW-123-U.女优.[标题].mp4',
        )

    def test_clearing_tags_strips_code_suffix(self):
        self.assertEqual(
            _compute_final_filename('ABW-123-UC.女优.[标题].mp4', False, False, False, None),
            'ABW-123.女优.[标题].mp4',
        )

    def test_multipart_filename_keeps_part_component(self):
        self.assertEqual(
            _compute_final_filename('ABW-123-C.女优.[标题].part1.mp4', True, False, False, None),
            'ABW-123-C.女优.[标题].part1.mp4',
        )


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
            self.assertEqual(orig.read_bytes(), b'original')
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

    def test_suffix_change_uses_new_code_returns_200(self):
        # Regression: replace_video_file re-suffixes the code (C→UC). The response
        # must look up the NEW code — the original request code no longer exists in
        # the DB, so get_video_by_code(old) → None → **None previously crashed (500).
        video_bytes = b'x'

        def fake_detail(c):
            return {'code': 'SSIS-001-UC'} if c == 'SSIS-001-UC' else None

        with tempfile.TemporaryDirectory() as tmpdir:
            orig = Path(tmpdir) / 'SSIS-001-C.mp4'
            orig.write_bytes(b'old')
            with patch('web.backend.app.replace_video_file',
                       return_value={'ok': True, 'new_code': 'SSIS-001-UC'}), \
                 patch('web.backend.app.get_video_by_code', side_effect=fake_detail), \
                 patch('web.backend.app.get_video_file_path', return_value=str(orig)):
                resp = self.client.post(
                    '/api/videos/SSIS-001-C/replace',
                    data={
                        'file': (io.BytesIO(video_bytes), 'u.mp4'),
                        'file_size': str(len(video_bytes)),
                        'dest_name': 'SSIS-001.mp4',
                        'subtitle_zh': '1', 'uncensored_leaked': '1',
                    },
                    content_type='multipart/form-data',
                )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()['code'], 'SSIS-001-UC')

    def test_replace_whole_removes_old_part_files(self):
        video_bytes = b'whole video'
        with tempfile.TemporaryDirectory() as tmpdir:
            part1 = Path(tmpdir) / 'ABW-123.part1.mp4'
            part2 = Path(tmpdir) / 'ABW-123.part2.mp4'
            part1.write_bytes(b'old1')
            part2.write_bytes(b'old2')

            def fake_replace(code, dest, sub_zh, leaked, native, file_meta=None, keep_part=None):
                self.assertEqual(code, 'ABW-123')
                self.assertEqual(Path(dest).name, 'ABW-123-C.mp4')
                self.assertEqual(keep_part, 'part1')
                return {
                    'ok': True,
                    'new_code': 'ABW-123-C',
                    'old_paths': [str(part1), str(part2)],
                }

            with patch('web.backend.app.replace_multipart_with_single_file', side_effect=fake_replace), \
                 patch('web.backend.app.get_video_by_code', return_value={'code': 'ABW-123-C', 'tags': [], 'actors': []}), \
                 patch('web.backend.app.get_video_file_path', return_value=str(part1)):
                resp = self.client.post(
                    '/api/videos/ABW-123/replace?part=part1',
                    data=video_bytes,
                    headers={
                        'X-Dest-Name': 'ABW-123.part1.mp4',
                        'X-File-Size': str(len(video_bytes)),
                        'X-Subtitle-Zh': '1',
                        'X-Replace-Whole': '1',
                    },
                )

            self.assertEqual(resp.status_code, 200)
            self.assertFalse(part1.exists())
            self.assertFalse(part2.exists())
            self.assertEqual((Path(tmpdir) / 'ABW-123-C.mp4').read_bytes(), video_bytes)


from dal.db_manager import _order_clause


class OrderClauseTests(unittest.TestCase):
    def test_mtime_sorts_by_file_mtime_desc(self):
        self.assertEqual(_order_clause('mtime'), "COALESCE(file_mtime, '') DESC, id DESC")

    def test_score_unchanged(self):
        self.assertEqual(
            _order_clause('score'),
            "COALESCE(score, 0) DESC, COALESCE(release_date, '') DESC, id DESC",
        )

    def test_default_is_release_date(self):
        self.assertEqual(_order_clause('date'), "COALESCE(release_date, '') DESC, id DESC")


from dal.db_manager import rename_video_file


class RenameVideoFileTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                part TEXT,
                original_file_path TEXT,
                updated_at TIMESTAMP
            );
        """)
        conn.execute(
            "INSERT INTO videos (code, part, original_file_path) "
            "VALUES ('ABP-119-C', NULL, '/vol/ABP-119-C.old.mp4')"
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_updates_path_and_code(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            result = rename_video_file('ABP-119-C', None, '/vol/ABP-119-UC.桃谷绘里香.[标题].mp4', 'ABP-119-UC')
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT code, original_file_path FROM videos WHERE id=1").fetchone()
        conn.close()
        self.assertEqual(row[0], 'ABP-119-UC')
        self.assertEqual(row[1], '/vol/ABP-119-UC.桃谷绘里香.[标题].mp4')

    def test_unknown_code_returns_error(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            result = rename_video_file('NOSUCH', None, '/vol/x.mp4', 'NOSUCH')
        self.assertFalse(result['ok'])


import tempfile as _tf
from unittest.mock import patch as _patch
from web.backend.app import app as flask_app


class RenameFileEndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()
        self.dir = _tf.mkdtemp()
        self.old = Path(self.dir) / 'ABP-119-C.old.mp4'
        self.old.write_bytes(b'x')

    def tearDown(self):
        import shutil
        shutil.rmtree(self.dir, ignore_errors=True)

    def _post(self, body):
        return self.client.post('/api/videos/ABP-119-C/rename-file', json=body)

    def test_renames_file_and_updates_db(self):
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)), \
             _patch('web.backend.app.rename_video_file', return_value={'ok': True}) as m_ren, \
             _patch('web.backend.app.get_video_by_code', return_value={'code': 'ABP-119-UC'}):
            resp = self._post({'part': None, 'code': 'ABP-119-UC', 'actors': ['桃谷绘里香'], 'title': '标题'})
        self.assertEqual(resp.status_code, 200)
        new = Path(self.dir) / 'ABP-119-UC.桃谷绘里香.[标题].mp4'
        self.assertTrue(new.exists())
        self.assertFalse(self.old.exists())
        m_ren.assert_called_once()

    def test_target_exists_returns_409(self):
        (Path(self.dir) / 'ABP-119-UC.[标题].mp4').write_bytes(b'y')
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)):
            resp = self._post({'part': None, 'code': 'ABP-119-UC', 'actors': [], 'title': '标题'})
        self.assertEqual(resp.status_code, 409)
        self.assertTrue(self.old.exists())

    def test_empty_code_returns_400(self):
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)):
            resp = self._post({'part': None, 'code': '   ', 'actors': [], 'title': 't'})
        self.assertEqual(resp.status_code, 400)

    def test_source_missing_returns_400(self):
        self.old.unlink()
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)):
            resp = self._post({'part': None, 'code': 'ABP-119-UC', 'actors': [], 'title': 't'})
        self.assertEqual(resp.status_code, 400)

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core import subtitle_cache
from core.browser_queue import RuntimeStore
from core.event_consumer import Consumer, execute_dependency
from tests.test_service_runtime import ControlledExecutor
from web.backend.app import app


@unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'), 'FFmpeg is required')
class SubtitleCacheTests(unittest.TestCase):
    def test_background_tracks_are_part_specific_and_invalidate_after_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'DEMO-100.part2.mkv'
            subtitles = root / 'zh.srt'
            subtitles.write_text('1\n00:00:00,000 --> 00:00:00,800\n字幕测试\n')
            subprocess.run([
                'ffmpeg', '-v', 'error', '-f', 'lavfi', '-i', 'color=s=64x64:d=1',
                '-i', str(subtitles), '-c:v', 'mpeg4', '-c:s', 'srt',
                '-metadata:s:s:0', 'language=zho', str(source),
            ], check=True, timeout=15)
            store = RuntimeStore(root / 'test.db')
            executor = ControlledExecutor()
            consumer = Consumer(store, executor)
            url = '/api/videos/DEMO-100/subtitles?part=part2'

            def video_path(code, part):
                return str(source) if part == 'part2' else None

            with patch.object(subtitle_cache, 'CACHE_ROOT', root / 'cache'), \
                 patch('web.backend.app.scrape_store', return_value=store), \
                 patch('core.service_manager.ensure_service'), \
                 patch('web.backend.app.get_video_file_path', side_effect=video_path):
                client = app.test_client()
                self.assertEqual(client.post(url).status_code, 202)
                self.assertEqual(client.post(url).status_code, 202)
                self.assertEqual(len(store.active_tasks()), 1)
                consumer.tick()
                consumer.tick()
                future, command = executor.calls[-1]
                self.assertEqual(command['service'], 'subtitle')
                future.set_result(execute_dependency(command))
                consumer.tick()
                response = client.get(url)
                self.assertEqual(response.status_code, 200)
                track, = response.get_json()['tracks']
                self.assertEqual(track['language'], 'zh')
                with client.get(track['url']) as response:
                    self.assertEqual(response.mimetype, 'text/vtt')
                    self.assertIn('字幕测试', response.get_data(as_text=True))
                self.assertEqual(client.get(url.replace('part2', 'part1')).get_json()['tracks'], [])

                new_file = root / 'replacement.mkv'
                shutil.copyfile(source, new_file)
                new_file.replace(source)
                self.assertEqual(client.get(track['url']).status_code, 404)
                self.assertEqual(client.post(url).status_code, 202)

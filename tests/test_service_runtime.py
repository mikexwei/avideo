"""Regression coverage for durable handoffs and independently controlled services."""
import importlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from datetime import datetime
from unittest.mock import MagicMock, patch

from core.browser_queue import BROWSER_TIMEZONE, RuntimeStore
from core.browser_service import BrowserService
from core.event_consumer import Consumer, StoredJob
from core.task_policy import snapshot
from core import series_catalog, workflows
from dal.db_manager import get_video_by_code, get_videos_by_series, list_all_series_with_count
from tests.test_add_video_api import _init_full_schema


class ControlledExecutor:
    def __init__(self):
        self.calls = []

    def submit(self, function, command):
        future = Future()
        self.calls.append((future, command))
        return future

    def shutdown(self, wait=True):
        pass


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = self.root / 'test.db'
        with sqlite3.connect(self.path) as conn:
            _init_full_schema(conn)
            for name, type_name in [('maker','TEXT'),('publisher','TEXT'),('series','TEXT'),('score','REAL'),('duration','TEXT'),('release_date','TEXT'),('cover_path','TEXT')]:
                conn.execute(f'ALTER TABLE videos ADD COLUMN {name} {type_name}')
            self.file = self.root / 'RBD-001.mp4'
            self.file.write_bytes(b'video')
            conn.execute('INSERT INTO videos(code,original_file_path) VALUES (?,?)', ('RBD-001', str(self.file)))
        self.store = RuntimeStore(self.path)
        self.executor = ControlledExecutor()
        self.consumer = Consumer(self.store, self.executor)
        self.browser = MagicMock()
        self.browser.scrape.return_value = {'title_jp': '作品', 'actors': '', 'categories': ''}
        self.browser.check_login.return_value = True
        self.service = BrowserService(self.store, self.browser)
        for target in ('core.workflows.DB_PATH', 'dal.db_manager.DB_PATH'):
            guard = patch(target, self.path)
            guard.start()
            self.addCleanup(guard.stop)

    def task(self, kind='login_status', code='', **payload):
        return self.store.submit(kind, code, payload)

    def dispatch(self, kind='login_status', code='', **payload):
        task_id = self.task(kind, code, **payload)
        self.consumer.tick()
        task = self.store.get(task_id)
        self.assertEqual(task['status'], 'WAITING')
        command_id = json.loads(task['checkpoint_json'])['waiting_command']
        return task_id, self.store.command(command_id)

    def complete_dependency(self, result):
        self.consumer.tick()
        future, command = self.executor.calls[-1]
        future.set_result(result)
        self.consumer.tick()
        return command

    def test_consumer_can_dispatch_second_task_while_first_waits_for_browser(self):
        first, _ = self.dispatch()
        second, _ = self.dispatch('open_login')
        self.assertEqual(self.store.get(first)['status'], 'WAITING')
        self.assertEqual(self.store.get(second)['status'], 'WAITING')
        self.browser.scrape.assert_not_called()

    def test_series_translation_is_durable_and_shared_by_name_across_makers(self):
        raw = '時間・停止'
        self.browser.scrape.return_value = {'title_jp': '作品', 'series': raw, 'maker': '片商甲'}
        video, _ = self.dispatch('video', 'RBD-001')
        self.service.tick()
        self.consumer.tick()
        with self.store.connect(write=True) as conn:
            conn.execute("INSERT INTO videos(code,original_file_path,series,maker) VALUES ('RBD-002','two.mp4',?,'片商乙')", (raw,))
            series_catalog.attach(conn, self.store, 'RBD-002', raw)
            entries = conn.execute('SELECT id_jp,name,status,task_id FROM series_cn ORDER BY id').fetchall()
        self.assertEqual(len(entries), 1)
        self.assertEqual((entries[0]['name'], entries[0]['status']), (None, 'TRANSLATING'))
        translation = entries[0]['task_id']
        self.consumer.tick()
        self.complete_dependency({})
        self.consumer.tick()
        self.assertEqual(self.store.get(video)['status'], 'DONE')
        self.consumer.tick()
        self.consumer.tick()
        future, command = self.executor.calls[-1]
        self.assertEqual(command['kind'], 'series_translate')
        future.set_exception(RuntimeError('模型不可用'))
        self.consumer.tick()
        self.assertEqual(self.store.get(translation)['status'], 'FAILED')
        self.assertEqual(get_video_by_code('RBD-001')['series_translation_status'], 'FAILED')
        self.store.retry_task(translation)
        self.assertEqual(get_video_by_code('RBD-001')['series_translation_status'], 'TRANSLATING')
        self.consumer.tick()
        self.complete_dependency('时间停止')
        self.assertEqual(get_video_by_code('RBD-001')['series'], raw)
        self.assertEqual(get_video_by_code('RBD-001')['series_zh'], '时间停止')
        self.assertEqual(get_video_by_code('RBD-002')['series_zh'], '时间停止')
        entry, = list_all_series_with_count()
        self.assertEqual(entry['count'], 2)

    def test_series_report_publication_is_explicit_idempotent_and_detects_conflicts(self):
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE videos SET series='一号',maker='甲' WHERE code='RBD-001'")
            conn.execute("INSERT INTO videos(code,original_file_path,series,maker) VALUES ('RBD-002','two.mp4','二号','乙')")
            conn.execute("INSERT INTO videos(code,original_file_path,series) VALUES ('RBD-003','three.mp4','三号')")
        with self.store.connect(write=True) as conn:
            for code, raw in (('RBD-001', '一号'), ('RBD-002', '二号'), ('RBD-003', '三号')):
                series_catalog.attach(conn, self.store, code, raw)
            self.assertEqual(series_catalog.report_snapshot(conn), [])
            conn.execute("UPDATE series_cn SET name='简体译名',status='DONE'")
        app = importlib.import_module('web.backend.app').app
        with patch('web.backend.admin_api.store', return_value=self.store), patch('web.backend.admin_api.ensure_worker'):
            client = app.test_client()
            base = '/api/admin/series/reports'
            ids = []
            for _ in range(2):
                task_id = client.post(base).get_json()['id']
                self.assertEqual(client.post(base).get_json()['id'], task_id)
                self.consumer.tick()
                with self.store.connect() as conn:
                    members = series_catalog.report_snapshot(conn)
                self.complete_dependency({'version': series_catalog.REPORT_VERSION, 'members': members, 'groups': []})
                self.assertEqual(client.get(f'{base}/{task_id}').get_json()['status'], 'DONE')
                ids.append(task_id)
            self.assertEqual(len(list_all_series_with_count()), 3)
            member_ids = [row['id'] for row in members]
            groups = [{'name': 'A-B', 'member_ids': member_ids, 'final_id': None}]
            url = f'{base}/{ids[0]}/publish'
            overlap = [{'name': 'A-B', 'member_ids': member_ids[:2]}, {'name': 'B-C', 'member_ids': member_ids[1:]}]
            response = client.post(url, json={'groups': overlap})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(client.get(f'{base}/{ids[0]}').get_json()['published_groups'], groups)
            self.assertEqual(client.post(url, json={'groups': overlap}).status_code, 200)
            self.assertEqual(client.post(url, json={'groups': groups}).status_code, 200)
            self.assertEqual(client.post(url, json={'groups': groups}).status_code, 200)
            self.assertEqual(client.post(f'{base}/{ids[1]}/publish', json={'groups': groups}).status_code, 409)
            entry, = list_all_series_with_count()
            self.assertEqual((entry['series'], entry['count']), ('A-B', 3))
            self.assertEqual(get_videos_by_series(final_id=entry['final_id'])['total'], 3)
            self.assertEqual(get_video_by_code('RBD-002')['series'], '二号')

    def test_series_review_merges_transitive_links_and_rejects_conflicting_targets(self):
        groups = [
            {'name': 'A-B', 'member_ids': [1, 2], 'final_id': 10},
            {'name': 'C-D', 'member_ids': [3, 4], 'final_id': None},
            {'name': 'B-C', 'member_ids': [2, 3], 'final_id': None},
            {'name': 'E-F', 'member_ids': [5, 6], 'final_id': None},
        ]
        self.assertEqual(series_catalog.combine_review_groups(groups), [
            {'name': 'A-B', 'member_ids': [1, 2, 3, 4], 'final_id': 10},
            {'name': 'E-F', 'member_ids': [5, 6], 'final_id': None},
        ])
        groups[1]['final_id'] = 20
        with self.assertRaisesRegex(ValueError, '不同的发布目标'):
            series_catalog.combine_review_groups(groups)

    def test_series_report_embeds_only_normalized_simplified_translations(self):
        members = [
            {'id': 1, 'raw_name': '時間ストップ', 'name_zh': ' 時間・停止 ', 'id_final': None, 'final_name': None},
            {'id': 2, 'raw_name': '女捜査官', 'name_zh': '女搜查官', 'id_final': None, 'final_name': None},
            {'id': 3, 'raw_name': '未翻訳の名前', 'name_zh': None, 'id_final': None, 'final_name': None},
        ]
        with patch('ollama.Client') as client:
            embed = client.return_value.embed
            embed.return_value = {'embeddings': [[1, 0], [0, 1]]}
            report = series_catalog.generate_report(members)
            embed.assert_called_once_with(model='bge-m3', input=['时间停止', '女搜查官'])
        self.assertEqual([row['raw_name'] for row in report['members']], ['時間ストップ', '女捜査官'])
        self.assertEqual(report['groups'][0]['member_ids'], [1, 2])

    def test_series_report_automatically_selects_only_scores_above_threshold(self):
        from math import sqrt
        members = [{'id': index, 'raw_name': f'原名{index}', 'name_zh': f'译名{index}',
                    'id_final': None, 'final_name': None} for index in range(1, 5)]
        boundary = series_catalog.AUTO_GROUP_THRESHOLD
        with patch('ollama.Client') as model:
            model.return_value.embed.return_value = {
                'embeddings': [[1, 0], [1, 0], [1, 0], [boundary, sqrt(1 - boundary ** 2)]]
            }
            report = series_catalog.generate_report(members)
        automatic = [row for row in report['groups'] if row['automatic']]
        self.assertEqual([row['member_ids'] for row in automatic], [[1, 2], [1, 3]])
        self.assertTrue(any(row['score'] == boundary and not row['automatic'] for row in report['groups']))

    def test_series_review_next_round_uses_draft_name_and_publishes_only_on_request(self):
        with self.store.connect(write=True) as conn:
            for index, raw in enumerate(('甲原名', '乙原名', '丙原名', '丁原名'), 1):
                code = f'RBD-{index:03}'
                if index != 1:
                    conn.execute('INSERT INTO videos(code,original_file_path) VALUES (?,?)', (code, f'{code}.mp4'))
                conn.execute('UPDATE videos SET series=? WHERE code=?', (raw, code))
                series_catalog.attach(conn, self.store, code, raw)
            for index, name in enumerate(('甲译名', '乙译名', '丙译名', '丁译名'), 1):
                conn.execute("""UPDATE scrape_tasks SET status='DONE',result_json=?
                    WHERE id=(SELECT task_id FROM series_cn WHERE id=?)""", (json.dumps(name), index))
            members = series_catalog.report_snapshot(conn)

        app = importlib.import_module('web.backend.app').app
        with patch('web.backend.admin_api.store', return_value=self.store), patch('web.backend.admin_api.ensure_worker'):
            client = app.test_client()
            base = '/api/admin/series/reports'
            first = client.post(base).get_json()['id']
            self.consumer.tick()
            self.complete_dependency({'version': series_catalog.REPORT_VERSION, 'members': members, 'groups': []})
            groups = [{'name': 'X', 'member_ids': [1, 2], 'final_id': None},
                      {'name': 'Y', 'member_ids': [3, 4], 'final_id': None}]
            next_url = f'{base}/{first}/next'
            response = client.post(next_url, json={'groups': groups})
            self.assertEqual(response.status_code, 202)
            second = response.get_json()['id']
            self.assertEqual(client.post(next_url, json={'groups': groups}).get_json()['id'], second)
            self.consumer.tick()  # Dispatch the second round.
            self.consumer.tick()  # Start its model call.
            future, command = self.executor.calls[-1]
            from core.event_consumer import execute_dependency
            with patch('ollama.Client') as model:
                embed = model.return_value.embed
                embed.return_value = {'embeddings': [[1, 0], [1, 0]]}
                future.set_result(execute_dependency(command))
                embed.assert_called_once_with(model='bge-m3', input=['X', 'Y'])
            self.consumer.tick()
            report = client.get(f'{base}/{second}').get_json()['result']
            self.assertEqual(report['round'], 2)
            self.assertNotIn('members', report)
            self.assertEqual([(unit['name'], unit['member_ids']) for unit in report['units']],
                             [('X', [1, 2]), ('Y', [3, 4])])
            self.assertEqual(report['groups'][0]['member_ids'], [1, 2, 3, 4])
            self.assertEqual(report['groups'][0]['comparison_names'], ['X', 'Y'])
            self.assertTrue(report['groups'][0]['automatic'])
            self.assertEqual(client.post(next_url, json={'groups': groups}).get_json()['id'], second)

            next_url = f'{base}/{second}/next'
            for invalid in (groups[:1], [{'name': '拆组', 'member_ids': [1, 3, 4]}]):
                self.assertEqual(client.post(next_url, json={'groups': invalid}).status_code, 400)
            merged = [{'name': 'E', 'member_ids': [1, 2, 3, 4], 'final_id': None}]
            response = client.post(next_url, json={'groups': merged})
            self.assertEqual(response.status_code, 202)
            third = response.get_json()['id']
            self.consumer.tick()
            self.consumer.tick()
            future, command = self.executor.calls[-1]
            with patch('ollama.Client') as model:
                embed = model.return_value.embed
                embed.return_value = {'embeddings': [[1, 0]]}
                future.set_result(execute_dependency(command))
                embed.assert_called_once_with(model='bge-m3', input=['E'])
            self.consumer.tick()
            report = client.get(f'{base}/{third}').get_json()['result']
            self.assertEqual(report['round'], 3)
            self.assertNotIn('members', report)
            self.assertEqual([unit['name'] for unit in report['units']], ['E'])
            self.assertEqual(report['groups'], [])
            self.assertEqual(len(list_all_series_with_count()), 4)
            self.assertEqual(client.post(f'{base}/{third}/publish', json={'groups': merged}).status_code, 200)
            entry, = list_all_series_with_count()
            self.assertEqual((entry['series'], entry['count']), ('E', 4))

    def test_series_name_migration_retains_links_translation_and_reviewed_group(self):
        path = self.root / 'legacy.db'
        with sqlite3.connect(path) as conn:
            conn.executescript("""
                CREATE TABLE series_jp(id INTEGER PRIMARY KEY, raw_name TEXT, scope TEXT,
                    maker TEXT, source_ref TEXT, UNIQUE(raw_name,scope));
                CREATE TABLE series_final(id INTEGER PRIMARY KEY, name TEXT, revision INTEGER DEFAULT 0);
                CREATE TABLE series_cn(id INTEGER PRIMARY KEY, id_jp INTEGER UNIQUE REFERENCES series_jp(id),
                    name TEXT, status TEXT, task_id TEXT, id_final INTEGER REFERENCES series_final(id));
                CREATE TABLE videos(id INTEGER PRIMARY KEY, series_jp_id INTEGER REFERENCES series_jp(id));
                INSERT INTO series_jp VALUES (1,'同名','maker:a','A',''), (2,'同名','maker:b','B','');
                INSERT INTO series_final(id,name) VALUES (1,'已审核名称');
                INSERT INTO series_cn VALUES (1,1,'简体译名','DONE',NULL,NULL), (2,2,NULL,'FAILED',NULL,1);
                INSERT INTO videos VALUES (1,1), (2,2);
            """)
        store = RuntimeStore(path)
        with store.connect() as conn:
            self.assertEqual([tuple(row) for row in conn.execute('SELECT * FROM series_jp')], [(2, '同名')])
            self.assertEqual([tuple(row) for row in conn.execute('SELECT id_jp,name,status,id_final FROM series_cn')],
                             [(2, '简体译名', 'DONE', 1)])
            self.assertEqual([row[0] for row in conn.execute('SELECT series_jp_id FROM videos ORDER BY id')], [2, 2])
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_published_result_survives_consumer_stop_and_duplicate_publish(self):
        task_id, command = self.dispatch()
        self.store.service('consumer', desired='stop')
        self.assertTrue(self.service.tick())
        self.assertEqual(self.store.get(task_id)['status'], 'WAITING')
        current = self.store.command(command['id'])
        self.assertFalse(self.store.publish(current, 'DONE', False))
        reopened = RuntimeStore(self.path)
        reopened.recover_service('consumer')
        self.assertTrue(self.store.consume_result())
        self.assertFalse(self.store.consume_result())
        self.consumer.tick()
        self.assertEqual(self.store.get(task_id)['status'], 'DONE')
        self.assertTrue(json.loads(self.store.get(task_id)['result_json']))

    def test_atomic_retry_does_not_deliver_transient_failure_to_workflow(self):
        task_id, command = self.dispatch()
        self.browser.check_login.side_effect = RuntimeError('temporary')
        self.service.tick()
        self.assertEqual(self.store.command(command['id'])['status'], 'PENDING')
        self.assertFalse(self.store.consume_result())
        self.assertEqual(self.store.get(task_id)['status'], 'WAITING')

    def test_forced_command_obeys_persisted_cooldown_after_restart(self):
        task_id = self.store.submit('login_status', forced=True)
        self.consumer.tick()
        self.store.set_settings('browser', {'next_allowed_at': time.time() + 60})
        self.assertFalse(self.service.tick())
        restarted = BrowserService(RuntimeStore(self.path), self.browser)
        self.assertFalse(restarted.tick())
        self.browser.check_login.assert_not_called()
        self.assertEqual(self.store.get(task_id)['status'], 'WAITING')

    def test_browser_rechecks_deleted_target_before_network_access(self):
        task_id, _ = self.dispatch('video', 'RBD-001')
        with self.store.connect(write=True) as conn:
            conn.execute('UPDATE videos SET deleted=1')
        self.service.tick()
        self.browser.scrape.assert_not_called()
        self.consumer.tick()
        self.assertEqual(self.store.get(task_id)['status'], 'SKIPPED')

    def test_browser_rechecks_relation_changes_before_network_access(self):
        self.dispatch('video', 'RBD-001')
        with self.store.connect(write=True) as conn:
            conn.execute("INSERT INTO tags(name) VALUES ('Changed')")
            conn.execute('INSERT INTO video_tag_link VALUES (1,1)')
        self.service.tick()
        self.browser.scrape.assert_not_called()

    def test_late_result_does_not_overwrite_manual_title_change(self):
        task_id, _ = self.dispatch('video', 'RBD-001')
        self.service.tick()
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE videos SET title_jp='manual'")
        self.consumer.tick()
        self.assertEqual(self.store.get(task_id)['status'], 'SKIPPED')
        with self.store.connect() as conn:
            self.assertEqual(conn.execute('SELECT title_jp FROM videos').fetchone()[0], 'manual')

    def test_cancelled_task_and_old_execution_result_cannot_resume(self):
        task_id, command = self.dispatch()
        leased = self.store.claim_command('browser', 'old')
        self.store.cancel_task(task_id)
        self.store.retry_task(task_id)
        self.consumer.tick()
        self.store.publish(leased, 'DONE', False)
        self.store.consume_result()
        self.assertEqual(self.store.get(task_id)['status'], 'WAITING')
        self.assertNotEqual(self.store.get(task_id)['execution_id'], command['execution_id'])

    def test_concurrent_command_claim_only_has_one_owner(self):
        self.dispatch()
        claimed = []
        def claim():
            row = self.store.claim_command('browser', threading.current_thread().name)
            if row:
                claimed.append(row)
        threads = [threading.Thread(target=claim) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(claimed), 1)

    def test_browser_recovery_preserves_cooldown_and_marks_interrupted_attempt(self):
        task_id, command = self.dispatch()
        leased = self.store.claim_command('browser', 'dead')
        self.store.set_settings('browser', {'next_allowed_at': time.time() + 50})
        self.store.recover_service('browser')
        self.assertEqual(self.store.command(command['id'])['status'], 'PENDING')
        self.assertFalse(self.store.publish(leased, 'DONE', True))
        self.assertGreater(self.store.settings('browser')['next_allowed_at'], time.time())
        self.assertFalse(self.store.consume_result())
        self.assertEqual(self.store.get(task_id)['status'], 'WAITING')

    def test_consumer_recovery_leaves_browser_command_running(self):
        self.dispatch()
        leased = self.store.claim_command('browser', 'browser')
        self.store.recover_service('consumer')
        self.store.recover_service('dependency')
        self.assertEqual(self.store.command(leased['id'])['status'], 'RUNNING')

    def test_priority_orders_browser_queue_and_fifo_breaks_ties(self):
        first, c1 = self.dispatch()
        second = self.store.submit('open_login', forced=True)
        self.consumer.tick()
        c2 = self.store.claim_command('browser', 'owner')
        self.assertEqual(c2['task_id'], second)
        self.store.publish(c2, 'DONE', True)
        self.assertEqual(self.store.claim_command('browser', 'owner')['id'], c1['id'])

    def test_workflows_for_same_video_are_serialized(self):
        first, _ = self.dispatch('video', 'RBD-001')
        second = self.task('pipeline_rescrape', codes=['RBD-001'])
        self.consumer.tick()
        self.assertEqual(self.store.get(first)['status'], 'WAITING')
        self.assertEqual(self.store.get(second)['status'], 'PENDING')
        with self.store.connect() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM dependency_commands').fetchone()[0], 1)

    def test_slow_translation_does_not_block_browser_result(self):
        translation = self.task('translate', 'RBD-001', title='original', codes=['RBD-001'])
        self.consumer.tick()
        self.consumer.tick()
        self.assertFalse(self.executor.calls[0][0].done())
        login, _ = self.dispatch()
        self.service.tick()
        self.consumer.tick()
        self.assertEqual(self.store.get(login)['status'], 'DONE')
        self.assertEqual(self.store.get(translation)['status'], 'WAITING')

    def test_complete_rescrape_runs_browser_translation_and_rename_in_stages(self):
        task_id, _ = self.dispatch('pipeline_rescrape', codes=['RBD-001'])
        self.service.tick()
        self.consumer.tick()  # apply scrape
        self.consumer.tick()  # submit metadata assets
        self.complete_dependency({})
        self.consumer.tick()  # submit translation
        self.complete_dependency('翻译标题')
        self.consumer.tick()  # submit short title
        self.complete_dependency('短标题')
        self.consumer.tick()  # rename
        self.consumer.tick()  # finish batch
        self.assertEqual(self.store.get(task_id)['status'], 'DONE')
        with self.store.connect() as conn:
            row = conn.execute('SELECT title_zh,title_zh_short,original_file_path FROM videos').fetchone()
        self.assertEqual(row['title_zh'], '翻译标题')
        self.assertEqual(row['title_zh_short'], '短标题')
        self.assertTrue(Path(row['original_file_path']).exists())
        self.assertIn('短标题', row['original_file_path'])

    def test_upload_translation_survives_concurrent_actor_avatar_update(self):
        from dal.db_manager import update_actor_avatar

        self.browser.scrape.return_value = {'title_jp': '作品', 'actors': 'New Actor', 'categories': ''}
        task_id, _ = self.dispatch('pipeline_auto', 'RBD-001', main_code='RBD-001', file_paths=[])
        self.service.tick()
        self.consumer.tick()  # Save the scraped data and enqueue the actor.
        self.consumer.tick()  # Submit metadata assets.
        self.complete_dependency({})
        self.consumer.tick()  # Submit translation with the original actor snapshot.
        with self.store.connect(write=True) as conn:
            actor_id = conn.execute("SELECT id FROM actors WHERE name='New Actor'").fetchone()[0]
            update_actor_avatar(actor_id, None, connection=conn)
            self.assertEqual(conn.execute('SELECT avatar_path FROM actors WHERE id=?', (actor_id,)).fetchone()[0], 'no_avatar')
        command = self.complete_dependency('翻译标题')
        self.assertEqual(command['kind'], 'translate')
        self.assertEqual(self.store.get(task_id)['status'], 'PENDING')
        self.consumer.tick()  # Submit short title.
        self.complete_dependency('短标题')
        self.consumer.tick()  # Rename and finish.
        self.assertEqual(self.store.get(task_id)['status'], 'DONE')
        with self.store.connect() as conn:
            row = conn.execute('SELECT title_zh,title_zh_short FROM videos').fetchone()
        self.assertEqual(tuple(row), ('翻译标题', '短标题'))

    def test_upload_translation_does_not_overwrite_concurrent_title_edit(self):
        task_id, _ = self.dispatch('pipeline_auto', 'RBD-001', main_code='RBD-001', file_paths=[])
        self.service.tick()
        self.consumer.tick()
        self.consumer.tick()
        self.complete_dependency({})
        self.consumer.tick()
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE videos SET title_zh='手工修改' WHERE code='RBD-001'")
        command = self.complete_dependency('旧译文')
        self.assertEqual(command['kind'], 'translate')
        self.assertEqual(self.store.get(task_id)['status'], 'SKIPPED')
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT title_zh FROM videos WHERE code='RBD-001'").fetchone()[0], '手工修改')

    def test_metadata_commit_and_stage_roll_back_together(self):
        task_id, _ = self.dispatch('video', 'RBD-001')
        self.service.tick()
        self.store.consume_result()
        task = self.store.claim_workflow('test')
        original = workflows.update_video_metadata
        def fail(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError('crash before phase commit')
        with patch.object(workflows, 'update_video_metadata', side_effect=fail):
            with self.assertRaises(RuntimeError):
                self.consumer.advance(task)
        with self.store.connect() as conn:
            self.assertIsNone(conn.execute('SELECT title_jp FROM videos').fetchone()[0])
        self.store.recover_service('consumer')
        self.consumer.tick()
        self.assertEqual(json.loads(self.store.get(task_id)['checkpoint_json'])['phase'], 'assets')

    def test_new_actor_is_queued_once_with_video_metadata_and_scraped_separately(self):
        with self.store.connect(write=True) as conn:
            conn.execute("INSERT INTO actors(name,avatar_path) VALUES ('Existing Actor','avatars/existing.jpg')")
        self.browser.scrape.return_value = {'title_jp': '作品', 'actors': 'New Actor,Existing Actor,New Actor'}
        self.browser.actor.return_value = {'avatar_url': None, 'name_zh': '新演员'}
        video_task, _ = self.dispatch('video', 'RBD-001')
        self.service.tick()

        original_submit = self.store.submit_actor
        def fail_after_enqueue(*args, **kwargs):
            original_submit(*args, **kwargs)
            raise RuntimeError('crash before phase commit')
        self.store.consume_result()
        task = self.store.claim_workflow('test')
        with patch.object(self.store, 'submit_actor', side_effect=fail_after_enqueue):
            with self.assertRaises(RuntimeError):
                self.consumer.advance(task)
        with self.store.connect() as conn:
            self.assertIsNone(conn.execute("SELECT id FROM actors WHERE name='New Actor'").fetchone())
            self.assertIsNone(conn.execute("SELECT id FROM scrape_tasks WHERE kind='actor'").fetchone())

        self.store.recover_service('consumer')
        self.consumer.tick()
        actor_task, = [row for row in self.store.active_tasks() if row['kind'] == 'actor']
        payload = json.loads(actor_task['payload_json'])
        self.assertEqual(payload['source_code'], 'RBD-001')
        self.assertEqual(actor_task['code'], 'New Actor')
        self.assertEqual(self.store.submit_actor('New Actor', payload['actor_id']), actor_task['id'])
        self.browser.actor.assert_not_called()

        self.consumer.tick()  # Dispatch video assets ahead of the actor workflow.
        self.complete_dependency({})
        self.consumer.tick()  # Finish the video, independently of the actor result.
        self.assertEqual(self.store.get(video_task)['status'], 'DONE')
        self.consumer.tick()
        self.store.set_settings('browser', {'next_allowed_at': 0})
        self.service.tick()
        self.consumer.tick()
        self.consumer.tick()
        self.browser.actor.assert_called_once_with('New Actor')
        self.assertEqual(self.store.get(actor_task['id'])['status'], 'DONE')
        with self.store.connect() as conn:
            actor = conn.execute("SELECT name_zh FROM actors WHERE id=?", (payload['actor_id'],)).fetchone()
            self.assertEqual(actor['name_zh'], '新演员')

    def test_upload_move_journal_recovers_after_files_moved_before_insert(self):
        source = self.root / '.upload_tmp' / 'job' / 'SSIS-100-C.mp4'
        source.parent.mkdir(parents=True)
        source.write_bytes(b'upload')
        task_id = self.task('pipeline_auto', file_paths=[str(source)], volume_path=str(self.root), uncensored_crack_tag='')
        job = StoredJob(self.store, task_id)
        with patch.object(workflows, 'batch_insert_scanned_videos', side_effect=RuntimeError('interruption')):
            with self.assertRaises(RuntimeError):
                workflows.prepare_upload(job, [source], self.root, '')
        self.assertFalse(source.exists())
        code = workflows.prepare_upload(job, [source], self.root, '')
        self.assertEqual(code, 'SSIS-100-C')
        with self.store.connect() as conn:
            rows = conn.execute('SELECT * FROM videos WHERE code=?', (code,)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertTrue(Path(rows[0]['original_file_path']).exists())

    def test_rename_refuses_to_overwrite_existing_target(self):
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE videos SET title_zh_short='Target'")
        target = self.root / 'RBD-001.[Target].mp4'
        # Resolve the existing filename builder rather than encode a second naming scheme.
        from utils.rename_and_move_videos import VideoRecord
        record = VideoRecord(1, 'RBD-001', None, self.file, 'Target', None, None, [])
        target = workflows._build_current_library_destination(record)
        target.write_bytes(b'other')
        with self.assertRaises(FileExistsError):
            workflows._rename_files_for_code('RBD-001')
        self.assertEqual(target.read_bytes(), b'other')
        self.assertEqual(self.file.read_bytes(), b'video')

    def test_ltx_is_suspended_and_never_claimed(self):
        from core.scrape_queue import QueueStore
        task_id = QueueStore(self.path).submit('ltx', 'RBD-001', forced=True)
        reopened = RuntimeStore(self.path)
        self.assertEqual(reopened.get(task_id)['status'], 'SUSPENDED')
        self.assertIsNone(reopened.claim_workflow('test'))

    def test_paused_consumer_does_not_advance_saved_browser_result(self):
        task_id, _ = self.dispatch()
        self.store.service('consumer', paused=1)
        self.service.tick()
        self.consumer.tick()
        self.assertEqual(self.store.get(task_id)['status'], 'WAITING')
        self.store.service('consumer', paused=0)
        self.consumer.tick()
        self.assertEqual(self.store.get(task_id)['status'], 'DONE')

    def test_recent_scrape_progress_lists_uploads_and_individual_batch_results(self):
        now = time.time()
        with patch('core.browser_queue.time.time', return_value=now - 86401):
            self.task('video', 'OLD')
        with patch('core.browser_queue.time.time', return_value=now - 10):
            self.task('video', 'BACK-001')
        with patch('core.browser_queue.time.time', return_value=now - 5):
            batch = self.task('pipeline_rescrape', codes=['BATCH-001', 'BATCH-002', 'BATCH-003'])
        upload = self.task('pipeline_auto', 'RBD-001', file_paths=['/library/.upload_tmp/RBD-001.mp4'])
        self.task('actor', 'ACTOR-001')
        with self.store.connect(write=True) as conn:
            checkpoint = {'index': 1, 'phase': 'translate', 'items': [{'code': 'BATCH-001', 'ok': False, 'message': '未匹配'}]}
            conn.execute("UPDATE scrape_tasks SET status='WAITING',checkpoint_json=? WHERE id=?", (json.dumps(checkpoint), batch))
            conn.execute("UPDATE videos SET title_zh='影片标题' WHERE code='RBD-001'")
        app = importlib.import_module('web.backend.app').app
        with patch('web.backend.scrape_api.store', return_value=self.store):
            response = app.test_client().get('/api/scrape-progress')
        self.assertEqual(response.status_code, 200)
        cards = response.get_json()
        self.assertEqual([(card['code'], card['status']) for card in cards], [
            ('RBD-001', 'PENDING'), ('BATCH-001', 'FAILED'), ('BATCH-002', 'WAITING'),
            ('BATCH-003', 'PENDING'), ('BACK-001', 'PENDING')
        ])
        self.assertEqual(cards[0]['id'], upload)
        self.assertEqual(cards[0]['files'], ['RBD-001.mp4'])
        self.assertEqual(cards[0]['video']['title_zh'], '影片标题')
        self.assertEqual(cards[1]['message'], '未匹配')
        self.assertEqual(cards[2]['phase'], 'translate')
        self.assertEqual(cards[3]['phase'], '')

    def test_admin_validates_settings_and_retains_existing_cooldown(self):
        app = importlib.import_module('web.backend.app').app
        future = time.time() + 120
        self.store.set_settings('browser', {'next_allowed_at': future})
        with patch('web.backend.admin_api.store', return_value=self.store):
            client = app.test_client()
            response = client.get('/admin')
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.location, '/#/admin')
            self.assertEqual(client.patch('/api/admin/browser/settings', json={'interval_min': 200, 'interval_max': 100}).status_code, 400)
            self.assertEqual(client.patch('/api/admin/browser/settings', json={'next_allowed_at': 0}).status_code, 400)
            response = client.patch('/api/admin/browser/settings', json={'interval_min': 10, 'interval_max': 20})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()['next_allowed_at'], future)
            self.assertEqual(client.post('/api/admin/services/browser/control', json={'action':'stop'}, headers={'Origin':'https://foreign.example'}).status_code, 403)

    def test_browser_quota_blocks_pending_commands_and_preserves_hourly_usage(self):
        app = importlib.import_module('web.backend.app').app
        peak = int(datetime(2026, 10, 12, 14, tzinfo=BROWSER_TIMEZONE).timestamp())
        with patch('web.backend.admin_api.store', return_value=self.store), patch('time.time', return_value=peak - 5) as clock:
            client = app.test_client()
            self.store.start_browser_access_tracking()
            response = client.put('/api/admin/browser/quota', json={'peak_hour': 14, 'peak_limit': 1, 'spread': 2})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()['effective_at'], peak)
            limits = response.get_json()['quota']['limits']
            self.assertEqual(limits[14], 1)
            self.assertEqual(limits[0], 0)
            self.assertEqual(limits[12], limits[16])
            self.assertEqual(client.put('/api/admin/browser/quota', json={'peak_hour': 14, 'peak_limit': True, 'spread': 2}).status_code, 400)

            clock.return_value = peak + 1
            self.assertIsNone(self.store.reserve_browser_access('refresh', 1))
            _, command = self.dispatch()
            clock.return_value = peak + 5
            self.assertFalse(self.service.tick())
            self.assertEqual(self.store.command(command['id'])['status'], 'PENDING')
            self.assertEqual(self.store.command(command['id'])['attempt'], 0)
            self.assertEqual(self.store.service('browser')['state'], 'hourly_limit')
            self.assertEqual(self.store.reserve_browser_access('refresh', 1), peak + 3600)

            response = client.get('/api/admin/browser/usage?date=2026-10-12').get_json()
            self.assertIsNone(response['hours'][12]['used'])
            self.assertIsNone(response['hours'][13]['allowed'])
            self.assertEqual(response['hours'][14]['allowed'], 1)
            self.assertEqual(response['hours'][14]['used'], 1)
            self.assertEqual(response['hours'][14]['kinds'], {'refresh': 1})
            self.assertIsNone(response['hours'][15]['used'])
            clock.return_value = peak + 3600
            self.assertTrue(self.service.tick())
            self.browser.check_login.assert_called_once()

            dst = client.get('/api/admin/browser/usage?date=2026-11-01').get_json()['hours']
            self.assertEqual(len(dst), 25)
            self.assertEqual([hour['label'] for hour in dst].count('01:00'), 2)

    def test_ltx_http_endpoints_return_disabled_without_submitting_work(self):
        app = importlib.import_module('web.backend.app').app
        client = app.test_client()
        for path in ('/api/ltx/queue', '/api/ltx/scrape/enqueue', '/api/ltx/scrape/dry-run', '/api/ltx/open-login'):
            self.assertEqual(client.post(path, json={}).status_code, 410)

    def test_upload_endpoint_performs_local_preparation_before_async_submission(self):
        module = importlib.import_module('web.backend.app')
        with patch.object(module, 'DB_PATH', self.path), patch.object(module, 'scrape_store', return_value=self.store), \
             patch.object(module, 'MEDIA_LIBRARIES', [self.root]), patch.object(module, 'ensure_worker'):
            response = module.app.test_client().post('/api/videos/add-auto', data={
                'volume_path': str(self.root), 'uncensored_crack_tag':'',
                'files[]': (io.BytesIO(b'upload'), 'SSIS-100-C.mp4')}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 202)
        task = self.store.get(response.get_json()['job_id'])
        self.assertEqual(task['status'], 'PENDING')
        self.assertEqual(task['code'], 'SSIS-100-C')
        with self.store.connect() as conn:
            row = conn.execute("SELECT original_file_path FROM videos WHERE code='SSIS-100-C'").fetchone()
        self.assertTrue(Path(row[0]).exists())
        self.browser.scrape.assert_not_called()

    def test_service_processes_restart_consumer_without_restarting_browser(self):
        import subprocess
        import sys
        runtime_root = self.root / 'runtime'
        (runtime_root / 'data').mkdir(parents=True)
        script = '''
import sys
from pathlib import Path
import config
config.DB_PATH = Path(sys.argv[1])
import core.service_manager as manager
manager.DB_PATH = config.DB_PATH
manager.ROOT = Path(sys.argv[2])
if sys.argv[3] == 'browser':
    import core.browser_service as browser
    class FakeBrowser:
        page = None
        def __init__(self, progress): pass
        def check_login(self): return True
        def close(self): pass
    browser.Browser = FakeBrowser
manager.run_service(sys.argv[3])
'''
        children = []
        def start(name):
            self.store.service(name, desired='run', paused=0)
            child = subprocess.Popen([sys.executable, '-c', script, str(self.path), str(runtime_root), name],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            children.append(child)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if self.store.service(name)['pid'] == child.pid and self.store.service(name)['alive']:
                    return child
                if child.poll() is not None:
                    self.fail(child.communicate()[1].decode())
                time.sleep(0.05)
            self.fail('service heartbeat did not arrive')
        try:
            browser = start('browser')
            consumer = start('consumer')
            self.store.service('consumer', desired='stop')
            consumer.wait(timeout=10)
            task_id, _ = self.dispatch()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                with self.store.connect() as conn:
                    count = conn.execute('SELECT COUNT(*) FROM dependency_results WHERE acknowledged_at IS NULL').fetchone()[0]
                if count:
                    break
                time.sleep(0.05)
            self.assertEqual(self.store.get(task_id)['status'], 'WAITING')
            self.assertEqual(count, 1)
            resumed = start('consumer')
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and self.store.get(task_id)['status'] != 'DONE':
                time.sleep(0.05)
            self.assertEqual(self.store.get(task_id)['status'], 'DONE')
            self.assertNotEqual(consumer.pid, resumed.pid)
            self.assertIsNone(browser.poll())
            self.assertEqual(self.store.service('browser')['pid'], browser.pid)
        finally:
            for name in ('consumer', 'browser'):
                self.store.service(name, desired='stop')
            for child in children:
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.terminate()
                    child.wait(timeout=5)
                child.communicate()

    def test_rename_recovers_file_moved_before_db_commit(self):
        task_id = self.task('pipeline_rescrape', codes=['RBD-001'])
        self.store.claim_workflow('test')
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE videos SET title_zh_short='Recovered'")
        job = StoredJob(self.store, task_id)
        rename = Path.rename
        def interrupted_rename(source, target):
            rename(source, target)
            raise RuntimeError('process exits after file move')
        with patch.object(Path, 'rename', interrupted_rename):
            with self.assertRaises(RuntimeError):
                workflows._rename_files_for_code('RBD-001', job)
        self.assertFalse(self.file.exists())
        with self.store.connect() as conn:
            self.assertEqual(conn.execute('SELECT original_file_path FROM videos').fetchone()[0], str(self.file))
        workflows._rename_files_for_code('RBD-001', job)
        with self.store.connect() as conn:
            path = conn.execute('SELECT original_file_path FROM videos').fetchone()[0]
        self.assertTrue(Path(path).exists())
        self.assertIn('Recovered', path)

    def test_cancel_between_result_validation_and_commit_prevents_business_write(self):
        task_id, _ = self.dispatch('video', 'RBD-001')
        self.service.tick()
        self.store.consume_result()
        task = self.store.claim_workflow('test')
        self.consumer.validate_result(task)
        self.store.cancel_task(task_id)
        from core.task_policy import TargetChanged
        with self.assertRaises(TargetChanged):
            self.consumer.advance(task)
        with self.store.connect() as conn:
            self.assertIsNone(conn.execute('SELECT title_jp FROM videos').fetchone()[0])

    def test_completed_batch_retry_only_schedules_failed_films(self):
        task_id = self.task('pipeline_rescrape', codes=['RBD-001', 'RBD-002'])
        with self.store.connect(write=True) as conn:
            checkpoint = {'items': [{'code':'RBD-001','ok':True}, {'code':'RBD-002','ok':False}],
                          'rename_moves': {'old':'new'}, 'rename_identity': {'old':[1,2,3]}}
            conn.execute("UPDATE scrape_tasks SET status='DONE',checkpoint_json=? WHERE id=?", (json.dumps(checkpoint), task_id))
        new_id = self.store.retry_task(task_id)
        self.assertNotEqual(new_id, task_id)
        new_task = self.store.get(new_id)
        self.assertEqual(json.loads(new_task['payload_json'])['codes'], ['RBD-002'])
        self.assertEqual(json.loads(new_task['checkpoint_json'])['rename_moves'], {'old':'new'})
        self.assertEqual(self.store.get(task_id)['status'], 'DONE')

    def test_browser_allows_login_control_while_paused_but_preserves_cooldown(self):
        task_id, command = self.dispatch()
        self.store.service('browser', paused=1)
        self.store.set_settings('browser', {'next_allowed_at': time.time() + 50})
        self.assertFalse(self.service.tick())
        self.store.set_settings('browser', {'next_allowed_at': 0})
        self.assertTrue(self.service.tick())
        self.consumer.tick()
        self.assertEqual(self.store.get(task_id)['status'], 'DONE')
        self.assertTrue(self.store.service('browser')['paused'])

    def test_sse_reconnect_only_replays_events_after_last_id(self):
        module = importlib.import_module('web.backend.app')
        task_id = self.task()
        self.store.event(task_id, {'step':'scrape','status':'running','message':'before reconnect'})
        first_id = self.store.events(task_id)[0]['id']
        self.store.event(task_id, {'step':'done','status':'done','message':'after reconnect'})
        with patch.object(module, 'scrape_store', return_value=self.store):
            response = module.app.test_client().get('/api/jobs/' + task_id + '/stream', headers={'Last-Event-ID':str(first_id)})
            text = response.get_data(as_text=True)
        self.assertNotIn('before reconnect', text)
        self.assertIn('after reconnect', text)
        self.assertIn('id:', text)

    def test_finalizing_upload_again_returns_existing_job_after_files_moved(self):
        import uuid
        module = importlib.import_module('web.backend.app')
        task_id = str(uuid.uuid4())
        self.store.submit('pipeline_auto', 'RBD-001', task_id=task_id)
        with patch.object(module, 'DB_PATH', self.path), patch.object(module, 'scrape_store', return_value=self.store), \
             patch.object(module, 'MEDIA_LIBRARIES', [self.root]), patch.object(module, 'prepare_upload') as prepare:
            response = module.app.test_client().post('/api/videos/add-auto', json={'upload_id':task_id,'volume_path':str(self.root)})
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.get_json()['job_id'], task_id)
        prepare.assert_not_called()

    def test_cancelled_browser_command_is_never_executed(self):
        task_id, _ = self.dispatch()
        self.store.cancel_task(task_id)
        self.assertFalse(self.service.tick())
        self.browser.check_login.assert_not_called()
        self.assertEqual(self.store.history(task_id)[0]['status'], 'CANCELLED')

    def test_retry_sse_does_not_replay_previous_execution_completion(self):
        task_id, _ = self.dispatch()
        self.store.event(task_id, {'step':'error','message':'old failure'})
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE scrape_tasks SET status='FAILED' WHERE id=?", (task_id,))
        self.store.retry_task(task_id)
        self.consumer.tick()
        self.store.event(task_id, {'step':'scrape','message':'new execution'})
        messages = [json.loads(row['event_json'])['message'] for row in self.store.events(task_id)]
        self.assertNotIn('old failure', messages)
        self.assertIn('new execution', messages)

    def test_translation_failure_keeps_existing_title_and_allows_local_completion(self):
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE videos SET title_zh='Existing title'")
        task_id, _ = self.dispatch('pipeline_rescrape', codes=['RBD-001'])
        self.service.tick()
        self.consumer.tick()
        self.consumer.tick()
        self.complete_dependency({})
        self.consumer.tick()
        self.consumer.tick()
        future, command = self.executor.calls[-1]
        self.assertEqual(command['kind'], 'translate')
        future.set_exception(TimeoutError('model timed out'))
        for _ in range(4):
            self.consumer.tick()
        self.assertEqual(self.store.get(task_id)['status'], 'DONE')
        with self.store.connect() as conn:
            self.assertEqual(conn.execute('SELECT title_zh FROM videos').fetchone()[0], 'Existing title')

    def test_daily_workflow_waits_for_video_then_actor_then_translation_jobs(self):
        video = self.task()
        parent = self.task('daily_finalize', video_tasks=[video])
        self.consumer.tick()  # video waits for browser
        with patch('core.auto_actor_scraper.run_actor_worker', return_value=[]) as actors:
            self.consumer.tick()  # parent defers while video is unfinished
            actors.assert_not_called()
        with self.store.connect(write=True) as conn:
            conn.execute("UPDATE scrape_tasks SET status='DONE' WHERE id=?", (video,))
            conn.execute('UPDATE scrape_tasks SET next_run_at=0 WHERE id=?', (parent,))
        with patch('core.auto_actor_scraper.run_actor_worker', return_value=[]) as actors:
            self.consumer.tick()
            actors.assert_called_once()
        with patch('utils.translate_titles.batch_translate_titles', return_value=[]) as translate:
            self.consumer.tick()
            translate.assert_called_once()
        self.consumer.tick()
        self.assertEqual(self.store.get(parent)['status'], 'DONE')

    def test_consumer_and_image_download_imports_do_not_load_playwright(self):
        import subprocess
        import sys
        result = subprocess.run([sys.executable, '-c',
            "import sys; import core.event_consumer; import core.scraper.assets; assert 'playwright.sync_api' not in sys.modules"],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

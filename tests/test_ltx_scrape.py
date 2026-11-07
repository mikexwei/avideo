import unittest
import importlib
import sqlite3
import tempfile
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.scraper.video_scraper import extract_ltx_links_from_reviews
from core.scraper import video_scraper
from core.ltx_scheduler import DAILY_LIMIT, HOURLY_LIMITS, TIMEZONE, quota
from core.scrape_queue import QueueStore, QueueConflict
from core.scrape_worker import Worker

app_module = importlib.import_module('web.backend.app')

class LtxEmptyReviewTests(unittest.TestCase):
    def test_zero_reviews_skip_click_network_wait_and_review_parsing(self):
        for label in ('短评(0)', '短評（0）', '短评 ( 0 )'):
            with self.subTest(label=label):
                page = MagicMock()
                tab = page.locator.return_value.first
                tab.count.return_value = 1
                tab.inner_text.return_value = label
                with patch.object(video_scraper, 'check_login_status', return_value=True), \
                     patch.object(video_scraper, 'scrape_video_info', return_value={'code': 'RBD-243'}), \
                     patch.object(video_scraper, 'simulate_human_behavior') as wait, \
                     patch.object(video_scraper, '_read_ltx_reviews') as read:
                    self.assertEqual(video_scraper.scrape_ltx_links(page, 'RBD-243'), [])
                tab.click.assert_not_called()
                page.expect_response.assert_not_called()
                wait.assert_not_called()
                read.assert_not_called()


class LtxQuotaTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db_path = Path(self.directory.name) / 'ltx.db'
        self.now = datetime(2026, 11, 5, 17, 15, tzinfo=TIMEZONE)

    def test_hourly_curve_sums_to_200_and_peaks_at_17(self):
        self.assertEqual(sum(HOURLY_LIMITS), DAILY_LIMIT)
        self.assertEqual(HOURLY_LIMITS.index(max(HOURLY_LIMITS)), 17)
        self.assertEqual(sorted(HOURLY_LIMITS[:18]), HOURLY_LIMITS[:18])
        self.assertEqual(sorted(HOURLY_LIMITS[17:], reverse=True), HOURLY_LIMITS[17:])

    def test_hourly_limit_survives_new_connections_and_resets_next_hour(self):
        for index in range(HOURLY_LIMITS[17]):
            self.assertTrue(quota(self.db_path, code=f'RBD-{index}', now=self.now)['allowed'])
        blocked = quota(self.db_path, code='EXTRA', now=self.now)
        self.assertFalse(blocked['allowed'])
        self.assertEqual(blocked['reason'], 'hourly_limit')
        self.assertEqual(blocked['daily_used'], HOURLY_LIMITS[17])
        self.assertTrue(quota(self.db_path, code='NEXT', now=self.now + timedelta(hours=1))['allowed'])

    def test_daily_limit_and_local_midnight(self):
        quota(self.db_path, now=self.now)
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany('INSERT INTO ltx_scrape_attempts(code, started_at) VALUES (?, ?)',
                             [(f'RBD-{i}', self.now.timestamp()) for i in range(200)])
        result = quota(self.db_path, code='EXTRA', now=self.now)
        self.assertFalse(result['allowed'])
        self.assertEqual(result['reason'], 'daily_limit')
        tomorrow = datetime(2026, 11, 6, 17, 0, tzinfo=TIMEZONE)
        self.assertEqual(quota(self.db_path, now=tomorrow)['daily_used'], 0)
        self.assertTrue(quota(self.db_path, code='NEXT', now=tomorrow)['allowed'])

    def test_reservation_is_atomic(self):
        quota(self.db_path, now=self.now)
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany('INSERT INTO ltx_scrape_attempts(code, started_at) VALUES (?, ?)',
                             [(f'RBD-{i}', self.now.timestamp()) for i in range(HOURLY_LIMITS[17] - 1)])
        results = []
        def reserve():
            results.append(quota(self.db_path, code='CONCURRENT', now=self.now)['allowed'])
        workers = [threading.Thread(target=reserve) for _ in range(4)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        self.assertEqual(sum(results), 1)

    def test_imports_existing_results_once(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('CREATE TABLE ltx_scrape_results(code TEXT, scraped_at TEXT)')
            conn.execute("INSERT INTO ltx_scrape_results VALUES ('RBD-001', datetime(?, 'unixepoch'))", (self.now.timestamp(),))
        self.assertEqual(quota(self.db_path, now=self.now)['daily_used'], 1)
        self.assertEqual(quota(self.db_path, now=self.now)['daily_used'], 1)



class DurableQueueTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'queue.db'
        self.store = QueueStore(self.path)
        with self.store.connect() as conn:
            conn.executescript("""
                CREATE TABLE videos(id INTEGER PRIMARY KEY,code TEXT,deleted INTEGER DEFAULT 0,score REAL,title_zh TEXT,title_jp TEXT,original_file_path TEXT,part TEXT,scrape_status TEXT);
                CREATE TABLE tags(id INTEGER PRIMARY KEY,name TEXT UNIQUE);
                CREATE TABLE video_tag_link(video_id INTEGER,tag_id INTEGER);
                CREATE TABLE actors(id INTEGER PRIMARY KEY,name TEXT,name_zh TEXT,is_ignored INTEGER DEFAULT 0);
                CREATE TABLE video_actor_link(video_id INTEGER,actor_id INTEGER);
                INSERT INTO videos(id,code,score,title_zh) VALUES (1,'RBD-918',4.5,'Title'),(2,'RBD-243',4.4,'Title'),(3,'DSVR-123',4.3,'VR');
                INSERT INTO actors(id,name,name_zh) VALUES (1,'Actor','演员');
                INSERT INTO video_actor_link VALUES (1,1),(2,1);
            """)
        self.browser = MagicMock()
        self.browser.scrape_ltx.return_value = ['magnet:?xt=urn:btih:GOOD']
        self.browser.scrape.return_value = {'title_jp':'Title'}
        self.worker = Worker(self.store, self.browser)

    def add(self, code='RBD-918', **kwargs):
        return self.store.enqueue([code], **kwargs)['ids'][0]

    def test_queue_persists_and_normalizes_duplicate_codes(self):
        self.store.enqueue(['rbd-918-uc','RBD-918','RBD-243'])
        reopened = QueueStore(self.path)
        self.assertEqual([row['code'] for row in reopened.list_tasks()], ['RBD-918','RBD-243'])
        self.assertEqual(reopened.enqueue(['RBD-918'])['added'],0)

    def test_history_and_previous_success_survive_rescrape_failure(self):
        task_id = self.add()
        with patch('core.scrape_worker.quota',return_value={'allowed':True}):
            self.worker.run_one()
        with self.assertRaises(QueueConflict):
            self.store.enqueue(['RBD-918'])
        self.store.enqueue(['RBD-918'],confirm=True)
        self.assertIn('GOOD',self.store.get(task_id)['result_json'])
        self.browser.scrape_ltx.side_effect = LookupError('not found')
        with patch('core.scrape_worker.quota',return_value={'allowed':True}):
            self.worker.run_one()
        self.assertEqual(self.store.get(task_id)['status'],'FAILED')
        self.assertIn('GOOD',self.store.get(task_id)['result_json'])
        self.assertEqual(len(self.store.history(task_id)),2)

    def test_latest_video_tag_is_rechecked_before_quota_or_browser(self):
        task_id=self.add()
        with self.store.connect() as conn:
            conn.execute("INSERT INTO tags VALUES (1,'LTX 无码破解')")
            conn.execute('INSERT INTO video_tag_link VALUES (1,1)')
        with patch('core.scrape_worker.quota') as quota:
            self.worker.run_one()
        self.assertEqual(self.store.get(task_id)['status'],'SKIPPED')
        quota.assert_not_called()
        self.browser.scrape_ltx.assert_not_called()

    def test_deleted_video_is_skipped_without_consuming_quota(self):
        task_id=self.add()
        with self.store.connect() as conn:
            conn.execute('UPDATE videos SET deleted=1 WHERE id=1')
        with patch('core.scrape_worker.quota') as quota:
            self.worker.run_one()
        self.assertEqual(self.store.get(task_id)['status'],'SKIPPED')
        quota.assert_not_called()

    def test_forced_task_bypasses_pause_filter_quota_and_human_waits(self):
        self.store.set_settings('control',{'paused':True})
        task_id=self.add('DSVR-123',forced=True)
        def scrape(code):
            video_scraper.simulate_human_behavior(MagicMock(),60,60)
            return ['magnet:?xt=urn:btih:FORCE']
        self.browser.scrape_ltx.side_effect=scrape
        with patch('core.scrape_worker.quota',return_value={'allowed':True}) as quota, \
             patch.object(video_scraper.time,'sleep') as sleep:
            self.worker.run_one()
        self.assertEqual(self.store.get(task_id)['status'],'DONE')
        self.assertTrue(quota.call_args.kwargs['force'])
        sleep.assert_not_called()
        self.assertTrue(self.store.settings('control')['paused'])

    def test_immediate_task_interrupts_normal_task_at_safe_checkpoint(self):
        first=self.add()
        calls=[]
        def scrape(code):
            calls.append(code)
            if len(calls)==1:
                self.add('RBD-243',forced=True)
                self.worker.check_control(force=True)
            return []
        self.browser.scrape_ltx.side_effect=scrape
        with patch('core.scrape_worker.quota',return_value={'allowed':True}):
            self.worker.run_one()
            self.assertEqual(self.store.get(first)['status'],'PENDING')
            self.worker.run_one()
            self.worker.run_one()
        self.assertEqual(calls,['RBD-918','RBD-243','RBD-918'])
        self.assertEqual(self.store.history(first)[-1]['status'],'INTERRUPTED')

    def test_web_tasks_are_not_blocked_by_ltx_quota(self):
        self.add()
        future=datetime.now(TIMEZONE)+timedelta(hours=1)
        with patch('core.scrape_worker.quota',return_value={'allowed':False,'reason':'hourly_limit','next_at':future.isoformat()}):
            self.worker.run_one()
        video=self.store.submit('video','RBD-243')
        with patch('core.scrape_worker.quota') as quota:
            self.worker.run_one()
        quota.assert_not_called()
        self.assertEqual(self.store.get(video)['status'],'DONE')
        self.browser.scrape.assert_called_once_with('RBD-243')

    def test_priority_bulk_order_and_cancel(self):
        first=self.add()
        second=self.add('RBD-243')
        self.store.bulk([second],'priority',100)
        self.assertEqual(self.store.claim('test')['id'],second)
        self.store.defer(second)
        self.store.bulk([first],'cancel')
        self.assertEqual(self.store.get(first)['status'],'CANCELLED')

    def test_recovery_preserves_pending_tasks_and_attempt_history(self):
        task_id=self.add()
        self.store.claim('old-worker')
        self.store.begin(task_id)
        reopened=QueueStore(self.path)
        reopened.recover()
        self.assertEqual(reopened.get(task_id)['status'],'PENDING')
        self.assertEqual(reopened.get(task_id)['attempts'],1)
        self.assertEqual(reopened.history(task_id)[0]['status'],'INTERRUPTED')

    def test_concurrent_claim_only_leases_task_once(self):
        self.add()
        claimed=[]
        def claim():
            row=self.store.claim(uuid.uuid4().hex)
            if row: claimed.append(row['id'])
        threads=[threading.Thread(target=claim) for _ in range(4)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(len(claimed),1)

    def test_cancel_running_task_records_cancellation_not_recovery(self):
        task_id=self.add()
        self.store.claim('worker')
        self.store.begin(task_id)
        self.store.bulk([task_id],'cancel')
        self.store.defer(task_id,error='cancelled',interrupted=True)
        self.assertEqual(self.store.get(task_id)['status'],'CANCELLED')
        self.assertEqual(self.store.history(task_id)[0]['status'],'CANCELLED')

    def test_changed_links_reset_download_marker(self):
        task_id=self.add()
        with patch('core.scrape_worker.quota',return_value={'allowed':True}):
            self.worker.run_one()
            with self.store.connect() as conn:
                conn.execute('UPDATE ltx_scrape_results SET downloaded=1')
            self.store.enqueue(['RBD-918'],confirm=True)
            self.browser.scrape_ltx.return_value=['magnet:?xt=urn:btih:NEW']
            self.worker.run_one()
        with self.store.connect() as conn:
            downloaded=conn.execute('SELECT downloaded FROM ltx_scrape_results').fetchone()[0]
        self.assertFalse(downloaded)
        self.assertEqual(len(self.store.history(task_id)),2)

    def test_api_confirms_rescrape_and_persists_bulk_changes(self):
        task_id=self.add()
        with patch('core.scrape_worker.quota',return_value={'allowed':True}):
            self.worker.run_one()
        with patch('web.backend.scrape_api.store',return_value=self.store), \
             patch('web.backend.scrape_api.ensure_worker'), \
             patch.object(app_module,'DB_PATH',self.path):
            client=app_module.app.test_client()
            conflict=client.post('/api/ltx/scrape/enqueue',json={'codes':['RBD-918']})
            self.assertEqual(conflict.status_code,409)
            self.assertTrue(conflict.get_json()['confirm_required'])
            confirmed=client.post('/api/ltx/scrape/enqueue',json={'codes':['RBD-918'],'confirm_rescrape':True})
            self.assertEqual(confirmed.status_code,200)
            self.assertEqual(client.post('/api/ltx/queue/bulk',json={'codes':['RBD-918'],'action':'priority','priority':123}).status_code,200)
            self.assertEqual(self.store.get(task_id)['priority'],123)
            self.assertEqual(client.get('/api/ltx/queue').get_json()[0]['queue_status'],'PENDING')
            self.assertEqual(len(client.get('/api/ltx/queue/'+task_id+'/history').get_json()),1)
            self.assertEqual(client.post('/api/worker/control',json={'action':'stop'},headers={'Origin':'https://other.example'}).status_code,403)
            self.assertEqual(client.post('/api/ltx/queue/bulk',json={'codes':['RBD-918'],'action':'cancel','priority':''}).status_code,200)

    def test_quota_adjustment_and_forced_overspend_are_persistent(self):
        now=datetime(2026,11,5,17,0,tzinfo=TIMEZONE)
        self.store.set_settings('quota',{'daily_limit':1,'peak_hour':17,'spread':4})
        first=quota(self.path,code='FIRST',now=now)
        self.assertTrue(first['allowed'])
        self.assertFalse(quota(self.path,code='BLOCKED',now=now)['allowed'])
        forced=quota(self.path,code='FORCED',now=now,force=True)
        self.assertEqual(forced['daily_used'],2)
        self.assertFalse(quota(self.path,code='NORMAL',now=now)['allowed'])

    def test_persistent_web_workflow_and_reconnectable_events(self):
        task_id=self.store.submit('pipeline_rescrape','RBD-918',{'codes':['RBD-918']})
        event={'step':'done','status':'done','message':'finished'}
        self.store.event(task_id,event)
        with patch.object(app_module,'scrape_store',return_value=QueueStore(self.path)):
            response=app_module.app.test_client().get('/api/jobs/'+task_id+'/stream')
        self.assertEqual(response.status_code,200)
        self.assertIn('finished',response.get_data(as_text=True))


class LtxLinkExtractionTests(unittest.TestCase):
    def test_extracts_ed2k_and_magnet_from_marked_reviews(self):
        reviews = [
            {'text': 'LTX 版 ed2k://|file|ABC.mp4|123|HASH|/', 'hrefs': []},
            {'text': 'lxt release', 'hrefs': ['magnet:?xt=urn:btih:ABC123']},
        ]

        self.assertEqual(extract_ltx_links_from_reviews(reviews), [
            'ed2k://|file|ABC.mp4|123|HASH|/',
            'magnet:?xt=urn:btih:ABC123',
        ])

    def test_ignores_links_without_marker_and_marker_without_link(self):
        reviews = [
            {'text': 'ordinary review ed2k://|file|ABC.mp4|123|HASH|/', 'hrefs': []},
            {'text': 'LTX but no download', 'hrefs': []},
        ]

        self.assertEqual(extract_ltx_links_from_reviews(reviews), [])

    def test_marker_in_one_review_cannot_qualify_adjacent_review_links(self):
        reviews = [
            {'text': 'ltx模型 ed2k://|file|ltx.mp4|1|LTXHASH|/', 'hrefs': []},
            {'text': 'Lada 32g ed2k://|file|lada.mp4|2|LADAHASH|/', 'hrefs': []},
            {'text': 'ed2k://|file|restored.mp4|3|OTHERHASH|/', 'hrefs': []},
        ]
        self.assertEqual(extract_ltx_links_from_reviews(reviews), [
            'ed2k://|file|ltx.mp4|1|LTXHASH|/',
        ])

    def test_deduplicates_links_case_insensitively(self):
        reviews = [
            {'text': 'LTX magnet:?xt=urn:btih:ABC', 'hrefs': ['MAGNET:?xt=urn:btih:ABC']},
        ]

        self.assertEqual(extract_ltx_links_from_reviews(reviews), ['MAGNET:?xt=urn:btih:ABC'])

    def test_marker_can_touch_chinese_text(self):
        reviews = [{'text': 'LTX破解 magnet:?xt=urn:btih:ABC', 'hrefs': []}]

        self.assertEqual(
            extract_ltx_links_from_reviews(reviews),
            ['magnet:?xt=urn:btih:ABC'],
        )



if __name__ == '__main__':
    unittest.main()

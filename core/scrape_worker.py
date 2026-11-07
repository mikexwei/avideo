"""Independent worker. This process alone owns the persistent JavDB browser."""
import fcntl
import json
import logging
import os
import random
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from config import DB_PATH
from core.ltx_scheduler import TIMEZONE, quota
from core.scrape_queue import QueueStore
from core.scraper.video_scraper import ScrapeInterrupted, scrape_control


ROOT = Path(__file__).resolve().parent.parent


class Browser:
    def __init__(self, progress):
        self.progress = progress
        self.context = None
        self.manager = None
        self.guard = None
        self.page = None

    def open(self):
        if self.context is not None:
            return
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        from core.scraper import javdb_profile_lock, launch_javdb_context
        self.guard = javdb_profile_lock(timeout=30)
        self.guard.__enter__()
        try:
            self.manager = sync_playwright().start()
            self.context = launch_javdb_context(self.manager, headless=False)
            self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
            Stealth().apply_stealth_sync(self.page)
        except BaseException:
            self.close()
            raise

    def live_page(self):
        self.open()
        if self.page.is_closed():
            from playwright_stealth import Stealth
            pages = [page for page in self.context.pages if not page.is_closed()]
            self.page = pages[-1] if pages else self.context.new_page()
            Stealth().apply_stealth_sync(self.page)
        return self.page

    def scrape(self, code):
        from core.scraper import scrape_video_info
        return scrape_video_info(self.live_page(), code, on_progress=self.progress)

    def scrape_ltx(self, code):
        from core.scraper import scrape_ltx_links
        return scrape_ltx_links(self.live_page(), code, on_progress=self.progress)

    def actor(self, name):
        from core.scraper import scrape_actor_info
        return scrape_actor_info(self.live_page(), name)

    def check_login(self):
        from core.scraper import check_login_status
        return check_login_status(self.live_page())

    def open_login(self):
        self.live_page().goto('https://javdb.com/login?locale=zh-CN', timeout=30000)
        return True

    def close(self):
        try:
            if self.context:
                self.context.close()
        finally:
            self.context = None
            try:
                if self.manager:
                    self.manager.stop()
            finally:
                self.manager = None
                if self.guard:
                    self.guard.__exit__(None, None, None)
                    self.guard = None


class StoredJob:
    def __init__(self, store, task_id):
        self.store, self.job_id = store, task_id
        self.queue = self
        self.status = 'running'
        self.failed = False
        self.error = None

    def put(self, event):
        self.store.event(self.job_id, event)
        if event.get('step') == 'error':
            self.failed, self.error = True, event.get('message')

    def checkpoint(self, value=None):
        return self.store.checkpoint(self.job_id, value)


class Worker:
    def __init__(self, store, browser=None):
        self.store = store
        self.owner = uuid.uuid4().hex
        self.browser = browser or Browser(self.progress)
        self.task = None
        self.state = 'idle'
        self.terminate = threading.Event()
        self.last_check = 0

    def progress(self, state, message):
        self.state = state
        self.store.worker_state(state=state, message=message, task_id=self.task['id'] if self.task else None,
                                code=self.task['code'] if self.task else None, next_at=None)
        self.check_control(force=True)

    def check_control(self, force=False):
        if not self.task or (not force and time.monotonic() - self.last_check < 0.5):
            return
        self.last_check = time.monotonic()
        row = self.store.get(self.task['id'])
        if row['cancel_requested'] or row['deleted']:
            raise ScrapeInterrupted('任务已取消或删除')
        self.task['forced'] = row['forced']
        if self.task['forced']:
            return
        control = self.store.settings('control')
        if self.task['kind'] == 'pipeline_rescrape' and self.state == 'waiting' and (
            control['paused'] or control['desired'] != 'run'
        ):
            raise ScrapeInterrupted('已在当前影片完成后暂停工作流')
        with self.store.connect() as conn:
            urgent = conn.execute("SELECT 1 FROM scrape_tasks WHERE status='PENDING' AND deleted=0 AND cancel_requested=0 AND forced=1 LIMIT 1").fetchone()
            yield_to_web = (self.task['kind'] == 'ltx' and self.state == 'cooling_down') or (
                self.task['kind'] == 'pipeline_rescrape' and self.state == 'waiting')
            interactive = yield_to_web and conn.execute(
                "SELECT 1 FROM scrape_tasks WHERE kind!='ltx' AND status='PENDING' AND deleted=0 AND cancel_requested=0 AND next_run_at<=? LIMIT 1", (time.time(),)
            ).fetchone()
        if urgent or interactive:
            raise ScrapeInterrupted('让出浏览器给高优先级任务')

    def run_one(self):
        task = self.store.claim(self.owner)
        if task is None:
            return False
        self.task = task
        try:
            if task['kind'] in ('ltx', 'ltx_preview'):
                reason = self.store.eligibility(task['code'], bool(task['forced']))
                if reason:
                    self.store.finish(task['id'], 'SKIPPED', error=reason)
                    return True
                self.check_control(force=True)
                allowance = quota(self.store.path, code=task['code'], force=bool(task['forced']))
                if not allowance['allowed']:
                    self.store.defer(task['id'], datetime.fromisoformat(allowance['next_at']).timestamp())
                    self.store.worker_state(state=allowance['reason'], message='等待刮削额度', next_at=allowance['next_at'],
                                            task_id=None, code=None)
                    return True
            self.store.begin(task['id'])
            self.progress('scraping', '正在执行 ' + task['code'])
            with scrape_control(self.check_control, lambda: bool(self.task['forced'])):
                result = self.execute(task)
            self.store.finish(task['id'], 'DONE', result)
        except ScrapeInterrupted as exc:
            self.store.defer(task['id'], error=str(exc), interrupted=True)
        except PermissionError as exc:
            self.store.defer(task['id'], error=str(exc), interrupted=True)
            with self.store.connect(write=True) as conn:
                conn.execute('UPDATE scrape_tasks SET forced=0 WHERE id=?', (task['id'],))
            self.store.set_settings('control', {'paused': True})
            self.store.worker_state(state='waiting_login', message=str(exc), task_id=None, code=None)
        except Exception as exc:
            logging.exception('Scrape task %s failed', task['id'])
            self.store.finish(task['id'], 'FAILED', error=str(exc))
            if task['kind'] == 'ltx' and task['retries'] < 2 and not isinstance(exc, (LookupError, ValueError)):
                with self.store.connect(write=True) as conn:
                    conn.execute("UPDATE scrape_tasks SET status='PENDING',retries=retries+1,next_run_at=? WHERE id=? AND cancel_requested=0",
                                 (time.time() + 60 * 2 ** task['retries'], task['id']))
        finally:
            self.task = None
        return True

    def execute(self, task):
        kind, code = task['kind'], task['code']
        if kind in ('ltx', 'ltx_preview'):
            return self.browser.scrape_ltx(code)
        if kind == 'video':
            return self.browser.scrape(code)
        if kind == 'actor':
            return self.browser.actor(code)
        if kind == 'login_status':
            return self.browser.check_login()
        if kind == 'open_login':
            return self.browser.open_login()
        if kind.startswith('pipeline_'):
            import importlib
            module = importlib.import_module('web.backend.app')
            module._worker_browser = self.browser
            job = StoredJob(self.store, task['id'])
            payload = json.loads(task['payload_json'])
            if kind == 'pipeline_auto':
                module._run_auto_pipeline(job, [Path(path) for path in payload['file_paths']], Path(payload['volume_path']),
                                          payload['uncensored_crack_tag'], payload['subtitle_classification'], payload['additional_tags'])
            elif kind == 'pipeline_rescrape':
                module._run_rescrape_pipeline(job, payload['codes'])
            else:
                raise ValueError('未知工作流类型')
            if job.failed:
                raise RuntimeError(job.error)
            return {'ok': True}
        raise ValueError('未知任务类型: ' + kind)

    def heartbeat(self):
        while not self.terminate.wait(3):
            try:
                self.store.worker_state(pid=os.getpid(), heartbeat=time.time())
            except Exception:
                logging.exception('Worker heartbeat failed')

    def run(self):
        self.store.recover()
        self.store.worker_state(pid=os.getpid(), heartbeat=time.time(), state='starting', message='worker 启动中', task_id=None, code=None)
        heart = threading.Thread(target=self.heartbeat, daemon=True)
        heart.start()
        refresh_at = time.monotonic() + random.uniform(1200, 2400)
        try:
            self.browser.open()
            while not self.terminate.is_set():
                control = self.store.settings('control')
                if control['desired'] in ('stop', 'restart'):
                    return 75 if control['desired'] == 'restart' else 0
                if self.run_one():
                    continue
                snapshot = self.store.worker_state()
                state = 'paused' if control['paused'] else 'idle'
                if control['paused'] and snapshot['state'] == 'waiting_login':
                    state = 'waiting_login'
                next_at = None
                if not control['paused']:
                    with self.store.connect() as conn:
                        waiting = conn.execute("SELECT MIN(next_run_at) FROM scrape_tasks WHERE status='PENDING' AND deleted=0 AND cancel_requested=0").fetchone()[0]
                    if waiting and waiting > time.time():
                        allowance = quota(self.store.path)
                        state = allowance['reason'] if not allowance['allowed'] else 'waiting'
                        next_at = allowance['next_at'] if not allowance['allowed'] else datetime.fromtimestamp(waiting, TIMEZONE).isoformat()
                self.store.worker_state(state=state, message={'paused': 'worker 已暂停', 'idle': '等待任务',
                                        'waiting_login': '等待手工登录'}.get(state, '等待下一可执行时间'), task_id=None, code=None, next_at=next_at)
                if time.monotonic() >= refresh_at:
                    page = self.browser.page
                    if page and not page.is_closed() and page.url.startswith('https://javdb.com'):
                        try:
                            page.reload(wait_until='domcontentloaded', timeout=30000)
                        except Exception:
                            logging.exception('Browser refresh failed')
                    refresh_at = time.monotonic() + random.uniform(1200, 2400)
                self.terminate.wait(0.5)
        finally:
            self.terminate.set()
            heart.join(timeout=5)
            self.browser.close()
            self.store.worker_state(state='stopped', message='worker 已停止', heartbeat=0, task_id=None, code=None)


def main():
    os.environ['AVIDEO_PROCESS_ROLE'] = 'worker'
    os.environ['AVIDEO_JAVDB_AUTOSTART'] = '0'
    store = QueueStore(DB_PATH)
    with (ROOT / 'data' / 'scrape_worker.lock').open('a+') as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        worker = Worker(store)
        signal.signal(signal.SIGTERM, lambda *_: worker.terminate.set())
        signal.signal(signal.SIGINT, lambda *_: worker.terminate.set())
        return worker.run()


def supervise():
    store = QueueStore(DB_PATH)
    with (ROOT / 'data' / 'scrape_supervisor.lock').open('a+') as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        while store.settings('control')['desired'] != 'stop':
            child = subprocess.Popen([sys.executable, '-m', 'core.scrape_worker'], cwd=ROOT)
            status = child.wait()
            if store.settings('control')['desired'] == 'stop':
                break
            if status == 75:
                store.set_settings('control', {'desired': 'run'})
            else:
                logging.warning('Worker exited (%s), restarting', status)
            time.sleep(2)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if '--supervisor' in sys.argv:
        supervise()
    else:
        sys.exit(main())

"""Claim registered browser requests and enforce the shared access budget."""
import logging
import random
import threading
import time
import uuid

from core.browser_service import BrowserBlocked, BrowserService
from core.scraper.video_scraper import ScrapeInterrupted
from core.task_policy import TargetChanged, validate_command


class BrowserConsumer:
    def __init__(self, store, browser=None):
        self.store = store
        self.owner = uuid.uuid4().hex
        self.service = BrowserService(self.progress, browser)
        self.kinds = tuple(self.service.handlers)
        self.current = None
        self.terminate = threading.Event()
        self.session_read_at = float('-inf')

    def checkpoint(self):
        if self.terminate.is_set():
            raise ScrapeInterrupted('浏览器服务退出')
        if self.current:
            validate_command(self.store, self.current)
            if self.current['kind'] not in ('javdb_listing', 'javdb_listing_detail'):
                kinds = tuple(kind for kind in self.kinds if kind != 'javdb_listing_detail')
                placeholders = ','.join('?' for _ in kinds)
                with self.store.connect() as conn:
                    urgent = conn.execute(f"""SELECT 1 FROM dependency_commands c JOIN scrape_tasks t ON t.id=c.task_id
                        WHERE c.service='browser' AND c.status='PENDING' AND c.next_run_at<=?
                        AND c.cancel_requested=0 AND t.execution_id=c.execution_id AND t.status='WAITING'
                        AND t.cancel_requested=0 AND t.deleted=0 AND t.paused=0
                        AND c.kind IN ({placeholders}) AND (c.priority>?
                            OR (c.kind='javdb_listing' AND ? NOT IN ('login_status','open_login'))) LIMIT 1""",
                        (time.time(), *kinds, self.current['priority'], self.current['kind'])).fetchone()
                if urgent:
                    raise ScrapeInterrupted('让出浏览器给高优先级命令')

    def progress(self, state, message):
        self.store.service('browser', state=state, message=message)
        if self.current:
            self.store.event(self.current['task_id'], {'step': 'scrape', 'status': 'running', 'message': message}, execution_id=self.current['execution_id'])
        self.checkpoint()

    def tick(self):
        control = self.store.service('browser')
        settings = self.store.settings('browser')
        now = time.monotonic()
        if now - self.session_read_at >= 3:
            self.session_read_at = now
            opened, logged_in = self.service.session_status()
            session = dict(session_open=opened, session_logged_in=logged_in, session_generation=control['generation'])
            if any(settings.get(key) != value for key, value in session.items()):
                self.store.set_settings('browser', session)
        if time.time() < settings['next_allowed_at']:
            self.store.service('browser', state='cooling_down', message=settings.get('cooldown_reason', '等待统一冷却结束'), current_id=None)
            return False
        if self.store.browser_quota_until():
            self.store.service('browser', state='hourly_limit', message='本小时访问额度已用尽', current_id=None)
            return False
        command = self.store.claim_command('browser', self.owner, kinds=self.kinds, control_only=bool(control['paused']))
        if not command:
            if not control['paused']:
                self.store.service('browser', state='idle', current_id=None)
            return False
        self.current = command
        self.store.service('browser', state='scraping', current_id=command['id'])
        try:
            payload = validate_command(self.store, command)
            if command['kind'] == 'javdb_listing_detail':
                from core.javdb_listing import local_codes
                with self.store.connect() as conn:
                    missing = conn.execute('SELECT 1 FROM javdb_listing_items WHERE code=? AND tags_json IS NULL',
                                           (payload['code'],)).fetchone()
                    needs_detail = missing and payload['code'] not in local_codes(conn)
                if not needs_detail:
                    self.store.publish(command, 'DONE', None)
                    return True
            # Reserve before any network access, so crashes and preemption retain the interval.
            until = self.store.reserve_browser_access(command['kind'], random.uniform(settings['interval_min'], settings['interval_max']))
            if until:
                self.store.publish(command, 'INTERRUPTED', error='等待浏览器冷却或小时额度恢复', retry_at=until)
                return True
            result = self.service.execute(command['kind'], payload, self.checkpoint)
            self.store.publish(command, 'DONE', result)
            if command['kind'] == 'login_status':
                self.store.set_settings('browser', {'logged_in': bool(result), 'login_checked_at': time.time()})
            self.store.set_settings('browser', {'failures': 0})
        except TargetChanged as exc:
            self.store.publish(command, 'SKIPPED', error=str(exc))
        except ScrapeInterrupted as exc:
            self.store.publish(command, 'INTERRUPTED', error=str(exc), retry_at=0)
        except PermissionError as exc:
            self.store.publish(command, 'LOGIN_REQUIRED', error=str(exc), retry_at=0)
            self.store.set_settings('browser', {'logged_in': False, 'login_checked_at': time.time(), 'session_logged_in': False})
            self.store.service('browser', paused=1, state='waiting_verification' if isinstance(exc, BrowserBlocked) else 'waiting_login', message=str(exc))
        except Exception as exc:
            logging.exception('Browser command %s failed', command['id'])
            failures = settings['failures'] + 1
            backoff = min(settings['backoff_max'], settings['backoff_base'] * 2 ** min(failures-1, 20))
            self.store.set_settings('browser', {'failures': failures, 'next_allowed_at': max(self.store.settings('browser')['next_allowed_at'], time.time() + backoff), 'cooldown_reason': str(exc)})
            self.store.publish(command, 'FAILED', error=str(exc),
                               retry_at=time.time() + backoff if command['attempt'] < 3 else None)
        finally:
            self.current = None
        return True

    def run(self):
        self.store.recover_service('browser')
        self.store.start_browser_access_tracking()
        refresh_at = time.monotonic() + random.uniform(1200, 2400)
        try:
            while not self.terminate.is_set():
                control = self.store.service('browser')
                if control['desired'] != 'run' or control['restart_token'] != control['generation']:
                    break
                if self.tick():
                    continue
                if not control['paused'] and time.monotonic() >= refresh_at:
                    from core.javdb_listing import active_task
                    with self.store.connect() as conn:
                        listing = active_task(conn)
                    settings = self.store.settings('browser')
                    if not listing and self.service.page_open and time.time() >= settings['next_allowed_at']:
                        until = self.store.reserve_browser_access('refresh', random.uniform(settings['interval_min'], settings['interval_max']))
                        if not until:
                            try:
                                self.service.refresh()
                            except Exception:
                                logging.exception('Idle page refresh failed')
                    refresh_at = time.monotonic() + random.uniform(1200, 2400)
                if control['paused']:
                    if control['state'] not in ('waiting_login', 'waiting_verification', 'hourly_limit'):
                        self.store.service('browser', state='paused', current_id=None)
                self.terminate.wait(0.25)
        finally:
            self.service.close()

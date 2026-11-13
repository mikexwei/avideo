"""Exclusive browser owner. Business work arrives only through durable commands."""
import logging
import random
import threading
import time
import uuid
from urllib.parse import urlsplit

from core.task_policy import TargetChanged, validate_command
from core.scraper.video_scraper import ScrapeInterrupted, scrape_control, browser_operation


class BrowserBlocked(PermissionError):
    pass


class SiteLimited(RuntimeError):
    pass


class Browser:
    def __init__(self, progress):
        self.progress = progress
        self.context = None
        self.manager = None
        self.guard = None
        self.page = None
        self.last_status = None

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
            self.track_page()
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
            self.track_page()
            Stealth().apply_stealth_sync(self.page)
        return self.page

    def track_page(self):
        def response_received(response):
            if response.request.is_navigation_request() and response.frame == self.page.main_frame:
                self.last_status = response.status
        self.page.on('response', response_received)

    def checked(self, result):
        from core.scraper.video_scraper import _is_cloudflare_challenge, _is_login_required
        if self.last_status == 429:
            raise SiteLimited('JavDB 返回 429，等待服务退避')
        if _is_cloudflare_challenge(self.page):
            raise BrowserBlocked('等待在浏览器中完成人工验证后恢复服务')
        if _is_login_required(self.page):
            raise PermissionError('等待在浏览器中完成登录后恢复服务')
        if self.last_status == 403:
            raise SiteLimited('JavDB 返回 403，等待服务退避')
        if self.last_status and self.last_status >= 500:
            raise RuntimeError(f'JavDB 返回 HTTP {self.last_status}')
        return result

    def scrape(self, code):
        from core.scraper import scrape_video_info
        return self.checked(scrape_video_info(self.live_page(), code, on_progress=self.progress))

    def collect_listing_page(self, number):
        from playwright.sync_api import TimeoutError
        from core.javdb_listing import list_url, read_page

        page = self.live_page()
        page.goto(list_url(number), wait_until='domcontentloaded', timeout=30000)
        self.checked(None)
        try:
            page.wait_for_selector('.movie-list .video-title strong, input[type="password"]',
                                   state='attached', timeout=15000)
        except TimeoutError:
            self.checked(None)
            raise
        self.checked(None)
        return read_page(page, number)

    def actor(self, name):
        from core.scraper import scrape_actor_info
        return self.checked(scrape_actor_info(self.live_page(), name))

    def check_login(self):
        from core.scraper import check_login_status
        return self.checked(check_login_status(self.live_page()))

    def open_login(self):
        self.live_page().goto('https://javdb.com/login?locale=zh-CN', timeout=30000)
        return True

    def session_status(self):
        from playwright.sync_api import Error
        from core.scraper.video_scraper import _is_login_required

        if self.context is None:
            return False, None
        pages = [page for page in self.context.pages if not page.is_closed()]
        if not pages:
            return False, None
        page = self.page if self.page in pages else pages[-1]
        if urlsplit(page.url).hostname not in ('javdb.com', 'www.javdb.com'):
            return True, None
        try:
            if _is_login_required(page):
                return True, False
            if page.locator('nav').count():
                return True, page.locator("a.navbar-item[href*='/users/']").count() > 0
        except Error:
            pass
        return True, None

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


class BrowserService:
    def __init__(self, store, browser=None):
        self.store = store
        self.owner = uuid.uuid4().hex
        self.browser = browser or Browser(self.progress)
        self.current = None
        self.terminate = threading.Event()
        self.session_read_at = float('-inf')

    def checkpoint(self):
        if self.terminate.is_set():
            raise ScrapeInterrupted('浏览器服务退出')
        if self.current:
            validate_command(self.store, self.current)
            # Only yield at scraper checkpoints. Every attempted command reserves cooldown first.
            with self.store.connect() as conn:
                urgent = conn.execute("SELECT 1 FROM dependency_commands WHERE service='browser' AND status='PENDING' AND priority>? AND cancel_requested=0 LIMIT 1",
                                      (self.current['priority'],)).fetchone()
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
            opened, logged_in = self.browser.session_status()
            session = dict(session_open=opened, session_logged_in=logged_in, session_generation=control['generation'])
            if any(settings.get(key) != value for key, value in session.items()):
                self.store.set_settings('browser', session)
        if time.time() < settings['next_allowed_at']:
            self.store.service('browser', state='cooling_down', message=settings.get('cooldown_reason', '等待统一冷却结束'), current_id=None)
            return False
        if self.store.browser_quota_until():
            self.store.service('browser', state='hourly_limit', message='本小时访问额度已用尽', current_id=None)
            return False
        command = self.store.claim_command('browser', self.owner, control_only=bool(control['paused']))
        if not command:
            if not control['paused']:
                self.store.service('browser', state='idle', current_id=None)
            return False
        self.current = command
        self.store.service('browser', state='scraping', current_id=command['id'])
        try:
            payload = validate_command(self.store, command)
            # Reserve before any network access, so crashes and preemption retain the interval.
            until = self.store.reserve_browser_access(command['kind'], random.uniform(settings['interval_min'], settings['interval_max']))
            if until:
                self.store.publish(command, 'INTERRUPTED', error='等待浏览器冷却或小时额度恢复', retry_at=until)
                return True
            code = payload.get('code', '')
            self.browser.last_status = None
            with scrape_control(self.checkpoint, lambda: False), browser_operation():
                kind = command['kind']
                if kind == 'video':
                    from core.scrape_queue import base_code
                    result = self.browser.scrape(base_code(code))
                elif kind == 'javdb_listing':
                    result = self.browser.collect_listing_page(payload['page'])
                elif kind == 'actor':
                    result = self.browser.actor(code)
                    if not isinstance(result, dict):
                        result = {'avatar_url': None, 'name_zh': result[1] if result else None}
                elif kind == 'login_status':
                    result = self.browser.check_login()
                elif kind == 'open_login':
                    result = self.browser.open_login()
                else:
                    raise ValueError('浏览器命令不受支持')
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
                    settings = self.store.settings('browser')
                    if self.browser.page and time.time() >= settings['next_allowed_at']:
                        until = self.store.reserve_browser_access('refresh', random.uniform(settings['interval_min'], settings['interval_max']))
                        if not until:
                            try:
                                self.browser.page.reload(wait_until='domcontentloaded', timeout=30000)
                            except Exception:
                                logging.exception('Idle page refresh failed')
                    refresh_at = time.monotonic() + random.uniform(1200, 2400)
                if control['paused']:
                    if control['state'] not in ('waiting_login', 'waiting_verification', 'hourly_limit'):
                        self.store.service('browser', state='paused', current_id=None)
                self.terminate.wait(0.25)
        finally:
            self.browser.close()

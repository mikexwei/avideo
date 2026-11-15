"""Execute browser requests with an exclusive Playwright session."""
from urllib.parse import urlsplit


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

    def collect_listing_detail(self, url):
        from playwright.sync_api import TimeoutError
        from core.javdb_listing import read_detail, valid_url

        if not valid_url(url, detail=True):
            raise ValueError('无效的 JavDB 详情链接')
        page = self.live_page()
        page.goto(url, wait_until='domcontentloaded', timeout=30000)
        self.checked(None)
        try:
            page.wait_for_selector('.movie-panel-info .panel-block, input[type="password"]',
                                   state='attached', timeout=15000)
        except TimeoutError:
            self.checked(None)
            raise
        self.checked(None)
        return read_detail(page, url)

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
    """Execute a supplied browser request on the thread that owns Playwright."""
    def __init__(self, progress, browser=None):
        self.progress = progress
        self.browser = browser or Browser(progress)

    def execute(self, kind, payload, checkpoint):
        from core.scraper.video_scraper import scrape_control, browser_operation

        self.browser.last_status = None
        with scrape_control(checkpoint, lambda: False), browser_operation():
            return self.handlers[kind](self, payload)

    def video(self, payload):
        from core.scrape_queue import base_code
        return self.browser.scrape(base_code(payload['code']))

    def actor(self, payload):
        result = self.browser.actor(payload['code'])
        if isinstance(result, dict):
            return result
        return {'avatar_url': None, 'name_zh': result[1] if result else None}

    def listing_page(self, payload):
        self.progress('scraping', f'正在读取 JavDB 列表第 {payload["page"]} 页')
        return self.browser.collect_listing_page(payload['page'])

    def listing_detail(self, payload):
        self.progress('scraping', f'正在读取 {payload["code"]} 的女优与标签')
        return self.browser.collect_listing_detail(payload['url'])

    @property
    def page_open(self):
        return self.browser.page is not None

    def refresh(self):
        self.browser.page.reload(wait_until='domcontentloaded', timeout=30000)

    def session_status(self):
        return self.browser.session_status()

    def close(self):
        self.browser.close()

    handlers = {
        'video': video,
        'actor': actor,
        'login_status': lambda service, _: service.browser.check_login(),
        'open_login': lambda service, _: service.browser.open_login(),
        'javdb_listing': listing_page,
        'javdb_listing_detail': listing_detail,
    }

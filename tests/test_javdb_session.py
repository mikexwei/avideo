import importlib
import threading
import unittest
from unittest.mock import MagicMock, patch

app_module = importlib.import_module('web.backend.app')
from web.backend.app import _JavdbSession, _next_javdb_refresh_delay


class _FakePage:
    def __init__(self):
        self.url = 'about:blank'
        self.thread_ids = []
        self.reload_calls = 0

    def goto(self, url):
        self.thread_ids.append(threading.get_ident())
        self.url = url

    def is_closed(self):
        return False

    def reload(self, **_kwargs):
        self.thread_ids.append(threading.get_ident())
        self.reload_calls += 1


class _FakeContext:
    def __init__(self, page):
        self.pages = [page]
        self.close_thread_id = None

    def new_page(self):
        page = _FakePage()
        self.pages.append(page)
        return page

    def close(self):
        self.close_thread_id = threading.get_ident()


class JavdbRefreshDelayTests(unittest.TestCase):
    def test_refresh_delay_uses_positive_and_negative_jitter(self):
        with patch.object(app_module.random, 'uniform', return_value=300), \
             patch.object(app_module.random, 'choice', return_value=-1):
            self.assertEqual(_next_javdb_refresh_delay(), 1500)
        with patch.object(app_module.random, 'uniform', return_value=600), \
             patch.object(app_module.random, 'choice', return_value=1):
            self.assertEqual(_next_javdb_refresh_delay(), 2400)


class JavdbSessionTests(unittest.TestCase):
    def test_browser_work_stays_on_its_owner_thread(self):
        page = _FakePage()
        context = _FakeContext(page)
        profile_guard = MagicMock()
        playwright_manager = MagicMock()
        scrape_thread_ids = []

        def fake_scrape(_page, code):
            scrape_thread_ids.append(threading.get_ident())
            return {'code': code}

        with patch('core.scraper.javdb_profile_lock', return_value=profile_guard), \
             patch('core.scraper.launch_javdb_context', return_value=context), \
             patch('core.scraper.load_javdb_cookies'), \
             patch('core.scraper.scrape_video_info', side_effect=fake_scrape), \
             patch('playwright.sync_api.sync_playwright', return_value=playwright_manager), \
             patch('playwright_stealth.Stealth'):
            session = _JavdbSession()
            session.start()
            self.assertEqual(session.scrape('MDVR-426'), {'code': 'MDVR-426'})
            session.close()

        owner_thread_id = page.thread_ids[0]
        self.assertNotEqual(owner_thread_id, threading.get_ident())
        self.assertEqual(scrape_thread_ids, [owner_thread_id])
        self.assertEqual(context.close_thread_id, owner_thread_id)

    def test_scrape_error_keeps_the_existing_session(self):
        session = MagicMock()
        session.scrape.side_effect = RuntimeError('blocked')

        with patch.object(app_module, '_javdb_session', session):
            with self.assertRaisesRegex(RuntimeError, 'blocked'):
                app_module._scrape_with_playwright('MDVR-426')
            self.assertIs(app_module._javdb_session, session)
            session.close.assert_not_called()

    def test_refresh_skips_blank_page_and_reloads_web_page(self):
        page = _FakePage()
        session = _JavdbSession()
        session.page = page
        session.context = _FakeContext(page)
        stealth = MagicMock()

        session._refresh(stealth)
        self.assertEqual(page.reload_calls, 0)

        page.url = 'https://javdb.com/'
        session._refresh(stealth)
        self.assertEqual(page.reload_calls, 1)


if __name__ == '__main__':
    unittest.main()

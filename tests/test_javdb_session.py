import threading
import unittest
from unittest.mock import MagicMock, patch

from core.scrape_worker import Browser
from core.scraper.video_scraper import scrape_control, _drop_session_and_reload
import importlib

app_module = importlib.import_module('web.backend.app')


class BrowserOwnershipTests(unittest.TestCase):
    def test_managed_browser_never_clears_manual_login_on_login_wall(self):
        page = MagicMock()
        with scrape_control(lambda: None, lambda: False):
            with self.assertRaises(PermissionError):
                _drop_session_and_reload(page, 'https://javdb.com/login', 'nav')
        page.context.clear_cookies.assert_not_called()
        page.goto.assert_not_called()

    def test_browser_operations_run_in_the_worker_calling_thread(self):
        owner = threading.get_ident()
        context = MagicMock()
        context.pages = [MagicMock()]
        calls = []
        with patch('core.scraper.javdb_profile_lock'), \
             patch('core.scraper.launch_javdb_context', return_value=context), \
             patch('playwright.sync_api.sync_playwright'), \
             patch('playwright_stealth.Stealth'), \
             patch('core.scraper.scrape_video_info', side_effect=lambda page, code, on_progress: calls.append(threading.get_ident()) or {'code':code}):
            browser = Browser(MagicMock())
            browser.open()
            self.assertEqual(browser.scrape('RBD-918'), {'code':'RBD-918'})
            browser.close()
        self.assertEqual(calls, [owner])
        context.close.assert_called_once()

    def test_web_gets_a_client_not_a_browser(self):
        client = MagicMock()
        with patch.object(app_module, '_worker_browser', None), \
             patch.object(app_module, '_javdb_session', None), \
             patch.object(app_module, 'WorkerClient', return_value=client):
            self.assertIs(app_module._get_javdb_session(), client)

    def test_pipeline_inside_worker_uses_its_owned_browser(self):
        browser = MagicMock()
        with patch.object(app_module, '_worker_browser', browser):
            app_module._scrape_with_playwright('RBD-918')
        browser.scrape.assert_called_once_with('RBD-918')


if __name__ == '__main__':
    unittest.main()

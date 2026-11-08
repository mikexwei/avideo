import threading
import unittest
from unittest.mock import MagicMock, patch

from core.browser_service import Browser
from core.scraper.video_scraper import parse_detail_page, scrape_control, _drop_session_and_reload
import importlib

app_module = importlib.import_module('web.backend.app')


class DetailLocaleTests(unittest.TestCase):
    def test_detail_fields_are_equivalent_in_simplified_and_traditional_chinese(self):
        values = ('2026-10-31', '120 分钟', 'Studio', 'Publisher', 'Series',
                  '4.5分, 由 15 人评分', 'Tag A', 'Actor A')
        labels = (
            ('日期 时长 片商 发行 系列 评分 类别 演员', '：'),
            ('日期 時長 片商 發行 系列 評分 類別 演員', ':'),
        )
        for names, separator in labels:
            with self.subTest(labels=names):
                page = MagicMock()
                title, cover, blocks = MagicMock(), MagicMock(), MagicMock()
                page.locator.side_effect = {
                    '.current-title': title, 'img.video-cover': cover, 'div.panel-block': blocks,
                }.__getitem__
                title.count.return_value = 1
                title.first.inner_text.return_value = 'Sample title'
                cover.count.return_value = 0
                items = []
                for label, value in zip(names.split(), values):
                    block = MagicMock()
                    block.inner_text.return_value = f'{label}{separator}\n{value}'
                    block.locator.return_value.inner_text.return_value = value
                    block.locator.return_value.all_inner_texts.return_value = [value]
                    if label == '系列':
                        untrusted, series_link = MagicMock(), MagicMock()
                        untrusted.get_attribute.return_value = 'https://example.com/series/other'
                        series_link.get_attribute.return_value = '/series/verified?locale=zh-CN'
                        block.locator.return_value.all.return_value = [untrusted, series_link]
                    items.append(block)
                blocks.count.return_value = len(items)
                blocks.nth.side_effect = items.__getitem__
                with patch('core.scraper.video_scraper._female_actor_names_from_block', return_value=['Actor A']) as extract_actors:
                    result = parse_detail_page(page, 'TEST-001')
                extract_actors.assert_called_once_with(items[-1])
                self.assertEqual(result['series_source_ref'], '/series/verified')
                self.assertEqual(
                    tuple(result[key] for key in ('release_date', 'duration', 'maker', 'publisher', 'series', 'score', 'categories', 'actors')),
                    (*values[:5], 4.5, *values[6:]),
                )


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
        page = MagicMock()
        page.url = 'https://javdb.com/'
        page.is_closed.return_value = False
        page.title.return_value = 'JavDB'
        page.locator.return_value.count.return_value = 0
        context.pages = [page]
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

    def test_web_has_no_owned_browser_or_synchronous_browser_proxy(self):
        self.assertFalse(hasattr(app_module, '_worker_browser'))
        self.assertFalse(hasattr(app_module, '_get_javdb_session'))
        self.assertFalse(hasattr(app_module, '_scrape_with_playwright'))


if __name__ == '__main__':
    unittest.main()

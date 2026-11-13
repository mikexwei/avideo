import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.browser_queue import RuntimeStore
from core.browser_service import BrowserService
from core.event_consumer import Consumer
from core.javdb_tag_check import candidates, filters, result_for_tags
from core.scraper.video_scraper import scrape_video_info
from tests.test_add_video_api import _init_full_schema
from web.backend.app import app


@pytest.fixture
def runtime(tmp_path):
    db = tmp_path / 'tasks.db'
    with sqlite3.connect(db) as conn:
        _init_full_schema(conn)
        tags = ('硬字幕', 'LTX 破解', 'LTX 无码破解', '原装无码', '無碼流出', 'VR', 'JAV 无码破解')
        conn.executemany('INSERT INTO tags(id,name) VALUES (?,?)', enumerate(tags, 1))
        conn.execute("INSERT INTO actors(id,name,name_zh) VALUES (1,'原名','译名')")
        for i, (code, links, deleted) in enumerate([
            ('ABC-001-UC', [1, 7], 0), ('ABC-001-UC', [1], 0),
            ('ABC-002-U', [1], 0), ('ABC-003-C', [1], 0), ('ABC-003-C', [2], 0),
            ('ABC-004-C', [1, 3], 0), ('ABC-005-C', [1, 4], 0),
            ('ABC-006-C', [1, 5], 0), ('ABC-007-C', [1, 6], 0),
            ('ABC-008-C', [], 0), ('ABC-009-C', [1], 1),
        ], 1):
            conn.execute('INSERT INTO videos(id,code,original_file_path,deleted) VALUES (?,?,?,?)',
                         (i, code, f'{i}.mp4', deleted))
            conn.executemany('INSERT INTO video_tag_link(video_id,tag_id) VALUES (?,?)', [(i, t) for t in links])
        conn.execute('INSERT INTO video_actor_link(video_id,actor_id) VALUES (2,1)')
        conn.execute("UPDATE videos SET title_zh='候选标题' WHERE id=1")
    return RuntimeStore(Path(db))


def test_candidate_filters_apply_to_whole_code(runtime):
    with runtime.connect() as conn:
        assert [row['code'] for row in candidates(conn, filters({}))] == ['ABC-001-UC', 'ABC-002-U']
        assert candidates(conn, filters({'actor': '原名', 'q': '标题', 'prefix': 'abc-001'})) == [
            {'code': 'ABC-001-UC', 'title': '候选标题', 'actors': ['译名'], 'checked': False},
        ]


def test_only_exact_normalized_detail_tag_counts():
    url = 'https://javdb.com/v/123'
    assert result_for_tags([' lTx  破解 '], url)['status'] == 'found'
    assert result_for_tags(['LTX 无码破解', 'LTX 破解讨论'], url)['status'] == 'absent'


def test_browser_reads_only_categories_of_the_matching_detail():
    page = MagicMock()
    page.url = 'https://javdb.com/v/123'
    blocks = page.locator.return_value
    blocks.count.return_value = 1
    block = blocks.nth.return_value
    block.inner_text.return_value = '类别：LTX 破解'
    block.locator.return_value.all_inner_texts.return_value = ['LTX 破解']
    module = 'core.scraper.video_scraper'
    with patch(module + '._open_video_search_page', return_value=True) as search, \
         patch(module + '._is_login_required', return_value=False), \
         patch(module + '._search_results_are_empty', return_value=False), \
         patch(module + '._apply_search_cooldown'), \
         patch(module + '._read_result_codes', return_value=['ABC-009', 'ABC-001']), \
         patch(module + '._open_result_detail', return_value=True) as detail, \
         patch(module + '.simulate_human_behavior'), \
         patch(module + '.read_ltx_magnets', return_value=['magnet:?xt=urn:btih:TEST']) as magnets, \
         patch(module + '.parse_detail_page') as metadata:
        result = scrape_video_info(page, 'ABC-001-UC', tags_only=True)
    search.assert_called_once_with(page, 'ABC-001')
    detail.assert_called_once_with(page, 'ABC-001-UC', 1)
    metadata.assert_not_called()
    magnets.assert_called_once_with(page)
    assert result == {'status': 'found', 'url': page.url, 'tag': 'LTX 破解', 'magnets': ['magnet:?xt=urn:btih:TEST']}


def test_task_api_preserves_progress_and_results_without_changing_video_tags(runtime):
    browser = MagicMock()
    browser.session_status.return_value = (True, True)
    browser.check_ltx_tag.side_effect = [
        result_for_tags(['LTX 破解'], 'https://javdb.com/v/123'),
        {'status': 'not_found', 'url': None, 'tag': None},
    ]
    service = BrowserService(runtime, browser)
    consumer = Consumer(runtime)
    client = app.test_client()
    with patch('web.backend.admin_api.store', return_value=runtime), patch('web.backend.admin_api.ensure_worker'), \
         patch.object(runtime, 'reserve_browser_access', return_value=None):
        first = client.post('/api/tasks/javdb-ltx-check')
        assert first.status_code == 202
        job = first.get_json()['id']
        assert client.post('/api/tasks/javdb-ltx-check').get_json()['id'] == job
        assert client.get('/api/tasks').get_json()['active']
        assert 'codes' not in json.loads(runtime.get(job)['payload_json'])
        assert client.delete(f'/api/admin/workflows/{job}').status_code == 409
        assert consumer.tick()
        assert client.post(f'/api/tasks/{job}/pause').status_code == 200
        assert client.get(f'/api/tasks/{job}').get_json()['status'] == 'PAUSED'
        assert not service.tick()
        assert client.post(f'/api/tasks/{job}/resume').status_code == 200
        assert service.tick()
        assert consumer.tick()
        interim = client.get(f'/api/tasks/{job}').get_json()
        assert interim['checked'] == 1
        assert interim['items'][0]['code'] == 'ABC-001-UC'
        assert consumer.tick()
        assert service.tick()
        assert consumer.tick()
        assert consumer.tick()
        final = client.get(f'/api/tasks/{job}?status=all').get_json()
        assert (final['status'], final['checked'], final['found']) == ('DONE', 2, 1)
        assert [row['status'] for row in final['items']] == ['found', 'not_found']
        assert [call.args[0] for call in browser.check_ltx_tag.call_args_list] == ['ABC-001-UC', 'ABC-002-U']
        assert not client.get('/api/tasks').get_json()['active']
        base = '/api/tasks/javdb-ltx-check/candidates'
        assert client.get(base).get_json()['total'] == 0
        assert client.get(base + '?checked=checked&prefix=ABC-002').get_json()['total'] == 1
        assert client.delete(f'/api/admin/workflows/{job}').status_code == 200
        assert client.get(f'/api/tasks/{job}').status_code == 404
        assert not client.get('/api/admin/workflows').get_json()['items']
        with runtime.connect() as conn:
            assert conn.execute('SELECT COUNT(*) FROM video_tag_link WHERE tag_id=2').fetchone()[0] == 1
    consumer.pool.shutdown()

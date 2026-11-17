import json
import sqlite3
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from core.browser_queue import BROWSER_TIMEZONE, RuntimeStore
from core.browser_consumer import BrowserConsumer
from core.event_consumer import Consumer
from core.javdb_listing import DETAIL_KIND, KIND, SCHEDULE, list_url, read_page, save_page, schedule_due
from tests.test_add_video_api import _init_full_schema
from web.backend.app import app


def entry(code):
    return {'code': code, 'title': 'JavDB ' + code, 'actors': [], 'url': 'https://javdb.com/v/' + code}


def test_read_page_distinguishes_last_page_from_parse_failure():
    page = MagicMock(url=list_url(1))
    parsed = {'lists': 1, 'entries': [entry('ABC-001')], 'next': list_url(2),
              'pagination': 1, 'current': ['1'], 'ambiguousNext': False}
    page.evaluate.return_value = parsed
    assert read_page(page, 1)['has_next']
    parsed['next'] = None
    assert not read_page(page, 1)['has_next']
    parsed['pagination'] = 0
    with pytest.raises(ValueError, match='无法确认'):
        read_page(page, 1)
    page.url = 'https://javdb.com/login'
    with pytest.raises(ValueError, match='跳转'):
        read_page(page, 1)


def test_localized_names_apply_to_history_filters_and_local_refresh(tmp_path):
    path = tmp_path / 'listing-names.db'
    with sqlite3.connect(path) as conn:
        _init_full_schema(conn)
        conn.executescript("""
            INSERT INTO actors(name,name_zh) VALUES
                ('羽生アリサ','羽生亞梨沙'), ('櫻美雪','櫻美雪'), ('原名のみ','');
            INSERT INTO tags(name) VALUES ('单体作品'), ('已婚妇女');
        """)
    runtime = RuntimeStore(path)
    with runtime.connect(write=True) as conn:
        runtime.submit(KIND, task_id='names', connection=conn)
        save_page(conn, 'names', 1, {'items': [entry('TEST-001')], 'has_next': False})
        conn.execute("""UPDATE javdb_listing_items SET actors_json=?,tags_json=?""", (
            json.dumps(['羽生アリサ', '羽生亞梨沙', '樱美雪', '原名のみ', '未知の女優']),
            json.dumps(['單體作品', '单体作品', '已婚婦女', '未登録タグ']),
        ))
    client = app.test_client()
    with patch('web.backend.admin_api.store', return_value=runtime):
        for endpoint in ('/api/tasks/names', '/api/tasks/javdb-listing/results'):
            row = client.get(endpoint).get_json()['items'][0]
            assert row['actors'] == ['羽生亚梨沙', '樱美雪', '原名のみ', '未知の女優']
            assert row['tags'] == ['单体作品', '已婚妇女', '未登録タグ']
            assert set(client.get(endpoint, query_string={'filter_options': 'tag'}).get_json()['options']) == set(row['tags'])
            for filters in (
                {'actor': '羽生亚梨沙', 'exact_actor': '1'},
                {'actor': '羽生アリサ'},
                {'tag': '单体作品', 'exact_tag': '1'},
                {'tag': '單體作品'},
            ):
                assert client.get(endpoint, query_string=filters).get_json()['result_total'] == 1
            assert client.get(endpoint, query_string={
                'tag': '单体作品', 'exact_tag': '1', 'exclude_tag': '1',
            }).get_json()['result_total'] == 0
        with runtime.connect(write=True) as conn:
            conn.execute("UPDATE actors SET name_zh='羽生新译名' WHERE name='羽生アリサ'")
        assert client.get('/api/tasks/names').get_json()['items'][0]['actors'][0] == '羽生新译名'


def test_collection_checkpoints_history_and_local_refresh(tmp_path):
    path = tmp_path / 'listing.db'
    with sqlite3.connect(path) as conn:
        _init_full_schema(conn)
        conn.executescript("""
            INSERT INTO videos(id,code,original_file_path,title_zh) VALUES
                (1,'ABC-001-C','one.mp4','本地标题'), (2,'ABC-001-UC','two.mp4','其他版本');
            INSERT INTO tags(id,name) VALUES (1,'硬字幕'), (2,'外挂 OCR 字幕'), (3,'LTX 无码破解');
            INSERT INTO video_tag_link VALUES (1,1), (2,2), (2,3);
            INSERT INTO actors(id,name,name_zh) VALUES (1,'演员原名','演员译名');
            INSERT INTO video_actor_link VALUES (1,1);
        """)
    store = RuntimeStore(path)
    with store.connect(write=True) as conn:
        for task_id, code, status in [('done','ABC-003','DONE'), ('failed','ABC-002','FAILED')]:
            store.submit(KIND, task_id=task_id, connection=conn)
            save_page(conn, task_id, 1, {'items': [entry(code)], 'has_next': False})
            conn.execute('UPDATE scrape_tasks SET status=? WHERE id=?', (status, task_id))
    browser = MagicMock()
    browser.session_status.return_value = (True, True)
    browser.collect_listing_page.side_effect = [
        {'items': [entry('ABC-001'), entry('ABC-001'), entry('ABC-002')], 'has_next': True},
        {'items': [entry('ABC-004'), entry('ABC-003'), entry('ABC-005')], 'has_next': True},
    ]
    browser.collect_listing_detail.return_value = {'actors': ['演员原名', '未知演员'], 'tags': ['类别 A', '类别 B']}
    service = BrowserConsumer(store, browser)
    client = app.test_client()
    with patch('web.backend.admin_api.store', return_value=store), patch('web.backend.admin_api.ensure_worker'), \
         patch.object(store, 'reserve_browser_access', return_value=None):
        task = client.post('/api/tasks/javdb-listing').get_json()['id']
        url = '/api/tasks/' + task
        consumer = Consumer(store)
        assert consumer.tick()
        assert service.tick()
        assert consumer.tick()
        assert client.get(url).get_json()['collected'] == 2
        assert client.post(url + '/pause').status_code == 200
        assert not consumer.tick()
        assert client.post(url + '/resume').status_code == 200
        consumer.pool.shutdown()
        consumer = Consumer(store)
        assert consumer.tick()
        assert service.tick()
        assert consumer.tick()
        assert not service.tick()
        assert consumer.tick()
        assert store.get(task)['status'] == 'DONE'
        browser.collect_listing_detail.assert_not_called()
        for _ in range(16):
            consumer.tick()
            service.tick()
            detail = client.get('/api/tasks').get_json()['detail']
            if detail['status'] == 'DONE':
                break
        consumer.pool.shutdown()
        assert [call.args for call in browser.collect_listing_page.call_args_list] == [(1,), (2,)]
        result = client.get(url).get_json()
        assert (result['status'], result['pages'], result['stop_reason']) == ('DONE', 2, 'history')
        assert [r['code'] for r in result['items']] == ['ABC-001', 'ABC-002', 'ABC-004']
        assert result['items'][0]['title'] == '本地标题'
        assert result['items'][0]['actors'] == ['演员译名']
        assert set(result['items'][0]['tags']) == {'硬字幕', '外挂 OCR 字幕', 'LTX 无码破解'}
        assert result['items'][1]['actors'] == ['演员译名', '未知演员']
        assert result['items'][1]['tags'] == ['类别 A', '类别 B']
        assert [call.args[0].split('/')[-1] for call in browser.collect_listing_detail.call_args_list] == [
            'ABC-002', 'ABC-004', 'ABC-003',
        ]
        assert client.get(url, query_string={'actor': '演员原名', 'in_db': 'yes'}).get_json()['result_total'] == 1
        assert client.get(url, query_string={'subtitle': '硬字幕', 'uncensored': 'LTX 无码破解'}).get_json()['result_total'] == 0
        assert client.get(url, query_string={'subtitle': '外挂 OCR 字幕', 'uncensored': 'LTX 无码破解'}).get_json()['result_total'] == 1
        results_url = '/api/tasks/javdb-listing/results'
        selected_codes = [('code', 'ABC-001'), ('code', 'ABC-004'), ('exact_code', '1')]
        for endpoint in (url, results_url):
            assert client.get(endpoint, query_string={'filter_options': 'actor'}).get_json() == {
                'options': ['未知演员', '演员译名'],
            }
            assert [r['code'] for r in client.get(endpoint, query_string=selected_codes).get_json()['items']] == [
                'ABC-001', 'ABC-004',
            ]
        inverted = client.get(url, query_string=selected_codes + [
            ('exclude_code', '1'), ('tag', '类别 A'), ('tag', '其他标签'), ('exact_tag', '1'),
        ]).get_json()
        assert [r['code'] for r in inverted['items']] == ['ABC-002']
        assert client.get(url, query_string=[
            ('subtitle', '硬字幕'), ('subtitle', '无字幕'), ('uncensored', 'LTX 无码破解'),
        ]).get_json()['result_total'] == 0
        assert client.get(url, query_string=[
            ('subtitle', '硬字幕'), ('subtitle', '外挂 OCR 字幕'), ('uncensored', 'LTX 无码破解'),
        ]).get_json()['result_total'] == 1
        with store.connect(write=True) as conn:
            conn.execute("UPDATE videos SET title_zh='更新标题' WHERE id=1")
        assert client.get(url, query_string={'q': '更新'}).get_json()['result_total'] == 1
        assert browser.collect_listing_page.call_count == 2
        assert client.get('/api/tasks/javdb-listing/results', query_string={
            'tag': '类别 A', 'exclude_tag': '1', 'actor': '演员原名',
        }).get_json()['result_total'] == 1
        assert client.delete(url, json={'keep_data': True}).status_code == 200
        assert client.get(url).status_code == 404
        assert client.get('/api/tasks/javdb-listing/results').get_json()['result_total'] == 4
        assert client.delete('/api/tasks/done', json={'keep_data': False}).status_code == 200
        assert client.get('/api/tasks/javdb-listing/results').get_json()['result_total'] == 3
        with store.connect(write=True) as conn:
            store.submit(KIND, task_id='next', connection=conn)
            assert save_page(conn, 'next', 1, {'items': [entry('ABC-001')], 'has_next': True}) == (0, 'history')


def test_listing_schedule(tmp_path):
    store = RuntimeStore(tmp_path / 'schedule.db')
    client = app.test_client()
    url = '/api/tasks/javdb-listing/schedule'
    now = datetime(2026, 11, 13, 8, 15, tzinfo=BROWSER_TIMEZONE)
    with patch('web.backend.admin_api.store', return_value=store), patch('web.backend.admin_api.ensure_worker'), \
         patch('web.backend.admin_api.time.time', return_value=now.replace(hour=7).timestamp()):
        assert client.get(url).get_json()['enabled'] is False
        assert client.post(url, json={'enabled': True, 'time': '25:00'}).status_code == 400
        assert client.post(url, json={'enabled': True, 'time': '08:15'}).status_code == 200
    assert not schedule_due(store, now.replace(minute=14))
    assert schedule_due(store, now)
    with patch('web.backend.admin_api.store', return_value=store):
        result = client.get('/api/tasks').get_json()
        assert result['latest']['trigger'] == 'scheduled'
        assert client.delete('/api/tasks/' + result['active_id'], json={'keep_data': False}).status_code == 409
    store.cancel_task(result['active_id'])
    assert not schedule_due(store, now.replace(minute=16))
    assert schedule_due(store, now.replace(day=14))
    assert store.settings(SCHEDULE)['last_day'] == '2026-11-14'


def test_schedule_takes_next_quota_slot_and_backfill_resumes_after_other_work(tmp_path):
    path = tmp_path / 'listing-priority.db'
    with sqlite3.connect(path) as conn:
        _init_full_schema(conn)
    store = RuntimeStore(path)
    browser = MagicMock()
    browser.session_status.return_value = (True, True)
    browser.collect_listing_page.side_effect = [
        {'items': [entry('NEW-002'), entry('NEW-002')], 'has_next': True},
        {'items': [entry('OLD-001')], 'has_next': True},
    ]
    browser.collect_listing_detail.return_value = {'actors': ['女优'], 'tags': ['类型']}
    browser.actor.return_value = {}
    consumer = Consumer(store)
    service = BrowserConsumer(store, browser)
    now = datetime(2026, 11, 15, 22, 58, tzinfo=BROWSER_TIMEZONE)
    hour = now.replace(minute=0).timestamp()

    with patch('time.time', return_value=now.timestamp()) as clock:
        with store.connect(write=True) as conn:
            store.submit(KIND, task_id='legacy', connection=conn)
            save_page(conn, 'legacy', 1, {'items': [entry('OLD-001')], 'has_next': False})
            conn.execute("INSERT INTO browser_quota VALUES (?,?)", (hour, json.dumps({'limits': [1, *([10] * 21), 1, 10]})))
            conn.execute("INSERT INTO browser_access(accessed_at,kind) VALUES (?,?)", (hour + 1, 'video'))
        task = store.claim_workflow('legacy')
        legacy_command = store.dispatch(task, '2:detail', 'browser', DETAIL_KIND,
                                        {'url': entry('OLD-001')['url'], 'code': 'OLD-001'},
                                        {'phase': 'detail', 'pages': 1, 'collected': 1,
                                         'stop_reason': 'last_page', 'current_code': 'OLD-001'})
        consumer.tick()
        assert store.get('legacy')['status'] == 'DONE'
        assert store.command(legacy_command)['status'] == 'CANCELLED'
        with store.connect() as conn:
            queued = conn.execute('SELECT id,status FROM scrape_tasks WHERE kind=?', (DETAIL_KIND,)).fetchone()
        assert queued['status'] == 'WAITING'
        assert not service.tick()

        store.set_settings(SCHEDULE, {'enabled': True, 'time': '22:58'})
        assert schedule_due(store, now)
        clock.return_value = hour + 3600
        assert not service.tick()  # Listing workflow exists before its first request is dispatched.
        assert consumer.tick()
        assert service.tick()
        browser.collect_listing_page.assert_called_once_with(1)
        browser.collect_listing_detail.assert_not_called()
        clock.return_value += 110
        assert not service.tick()  # First page result awaits Consumer acknowledgement.
        assert consumer.tick()
        assert consumer.tick()
        assert service.tick()
        assert consumer.tick()
        assert consumer.tick()
        assert browser.collect_listing_page.call_count == 2

        actor_id = store.submit('actor', '演员')
        assert consumer.tick()
        clock.return_value += 110
        assert service.tick()
        browser.actor.assert_called_once_with('演员')
        browser.collect_listing_detail.assert_not_called()
        assert consumer.tick()
        assert store.get(actor_id)['status'] == 'DONE'

        for _ in range(8):
            clock.return_value += 110
            service.tick()
            consumer.tick()
            if store.get(queued['id'])['status'] == 'DONE':
                break
        consumer.pool.shutdown()
        assert store.get(queued['id'])['status'] == 'DONE'
        assert [call.args for call in browser.collect_listing_detail.call_args_list] == [
            (entry('OLD-001')['url'],), (entry('NEW-002')['url'],),
        ]
        with store.connect() as conn:
            assert conn.execute('SELECT COUNT(*) FROM scrape_tasks WHERE kind=?', (DETAIL_KIND,)).fetchone()[0] == 1

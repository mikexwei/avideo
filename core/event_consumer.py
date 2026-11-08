"""Advance durable workflows without waiting for a browser or an external model."""
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from core import series_catalog, workflows
from core.scrape_queue import StoredJob
from core.task_policy import TargetChanged, matches_video_snapshot, snapshot


def execute_dependency(command):
    """Only external work. Business writes happen on the Consumer thread."""
    from core.dependency_executor import execute
    return execute(command['kind'], json.loads(command['payload_json']))


class Consumer:
    def __init__(self, store, executor=None):
        self.store = store
        self.owner = uuid.uuid4().hex
        self.pool = executor or ThreadPoolExecutor(max_workers=2, thread_name_prefix='external-dependency')
        self.futures = {}
        self.terminate = threading.Event()

    def emit(self, task, step, status, message, **extra):
        self.store.event(task['id'], dict(step=step, status=status, message=message, **extra), execution_id=task['execution_id'])

    def request(self, task, state, service, kind, payload):
        self.emit(task, state['phase'], 'running', '等待外部任务执行')
        self.store.dispatch(task, str(state.get('index', 0)) + ':' + state['phase'], service, kind, payload, state)

    def response(self, state):
        response = state.pop('response')
        if response['outcome'] == 'DONE':
            return response['result']
        if response['outcome'] == 'SKIPPED':
            raise TargetChanged(response['error'] or '目标失效')
        raise RuntimeError(response['error'] or response['outcome'])

    def finish(self, task, state):
        self.store.finish(task['id'], 'DONE', state.get('result', {'ok': True}))
        self.store.release_targets(task['id'])
        if self.store.get(task['id'])['status'] != 'DONE':
            self.emit(task, 'error', 'error', '任务已取消')
            return
        if task['kind'] == 'pipeline_rescrape':
            results = state.get('items', [])
            success = sum(item['ok'] for item in results)
            message = f'完成：{success} 成功 / {len(results) - success} 失败'
            self.emit(task, 'done', 'done', message, total=len(results), success=success, failed=len(results)-success)
        else:
            self.emit(task, 'done', 'done', '全部完成', code=state.get('code', task['code']))
        if task['kind'] == 'pipeline_auto':
            import shutil
            payload = json.loads(task['payload_json'])
            for path in payload['file_paths']:
                parent = Path(path).parent
                if parent.parent.name == '.upload_tmp':
                    shutil.rmtree(parent, ignore_errors=True)

    def commit_phase(self, task, state, apply):
        # Metadata/field writes and the next durable stage commit together.
        with self.store.connect(write=True) as conn:
            current = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (task['id'],)).fetchone()
            if current['status'] != 'RUNNING' or current['cancel_requested'] or current['deleted'] or current['execution_id'] != task['execution_id']:
                raise TargetChanged('任务已取消或执行版本已改变')
            old = json.loads(task['checkpoint_json'])
            payload = {}
            if 'response' in old:
                command = conn.execute('SELECT payload_json FROM dependency_commands WHERE execution_id=? AND id=?',
                                       (task['execution_id'], old['response']['command_id'])).fetchone()
                payload = json.loads(command['payload_json']) if command else {}
                expected = payload.get('snapshot')
                if expected is not None and not matches_video_snapshot(self.store, payload['code'], expected, connection=conn):
                    raise TargetChanged('影片已改变，拒绝提交旧结果')
            if payload.get('actor_snapshot') is not None:
                actor = conn.execute('SELECT * FROM actors WHERE id=?', (payload['actor_snapshot']['id'],)).fetchone()
                if actor is None or dict(actor) != payload['actor_snapshot']:
                    raise TargetChanged('演员在等待期间已改变')
            apply(conn)
            conn.execute("UPDATE scrape_tasks SET checkpoint_json=?,status='PENDING',owner=NULL WHERE id=? AND status='RUNNING'",
                         (json.dumps(state, ensure_ascii=False), task['id']))

    def advance_daily(self, task, state, payload):
        phase = state.get('phase', 'start')
        ids = payload.get('video_tasks', []) if phase == 'start' else state.get('children', [])
        active = {'PENDING', 'RUNNING', 'WAITING', 'PREPARING'}
        if any((self.store.get(task_id) or {}).get('status') in active for task_id in ids):
            self.store.defer(task['id'], until=time.time() + 1)
            return
        if phase == 'start':
            from core.auto_actor_scraper import run_actor_worker
            state.update(phase='actors', children=run_actor_worker())
            self.store.save_phase(task['id'], state)
        elif phase == 'actors':
            from utils.translate_titles import batch_translate_titles
            state.update(phase='translations', children=batch_translate_titles() or [])
            self.store.save_phase(task['id'], state)
        else:
            self.finish(task, state)

    def advance(self, task):
        payload = json.loads(task['payload_json'])
        state = json.loads(task['checkpoint_json'])
        state.setdefault('phase', 'start')
        job = StoredJob(self.store, task['id'], execution_id=task['execution_id'])
        kind = task['kind']
        if kind == 'daily_finalize':
            self.advance_daily(task, state, payload)
            return
        target_code = state.get('code') or task['code']
        if kind == 'pipeline_rescrape':
            codes = payload['codes']
            target_code = codes[state.get('index', 0)] if state.get('index', 0) < len(codes) else ''
        if target_code and kind not in ('login_status', 'open_login', 'series_translate', 'series_report', 'subtitle_cache'):
            from core.scrape_queue import base_code
            target = ('actor:' if kind == 'actor' else 'video:') + base_code(target_code)
            if not self.store.acquire_target(task['id'], target):
                self.store.defer(task['id'], until=time.time() + 0.5)
                return
        phase = state['phase']
        if phase == 'start':
            if kind == 'pipeline_auto':
                state['code'] = payload.get('main_code') or workflows.prepare_upload(
                    job, [Path(path) for path in payload['file_paths']], Path(payload['volume_path']),
                    payload.get('uncensored_crack_tag', ''), payload.get('subtitle_classification'), payload.get('additional_tags'))
                state.update(job.checkpoint())
            elif kind == 'pipeline_rescrape':
                index = state.get('index', 0)
                if index >= len(payload['codes']):
                    self.finish(task, state)
                    return
                state['code'] = payload['codes'][index]
                self.emit(task, state['code'], 'running', '刮削中…')
            elif kind == 'video':
                state['code'] = task['code']
            elif kind == 'subtitle_cache':
                state['phase'] = 'direct'
                self.request(task, state, 'subtitle', kind, payload)
                return
            elif kind in ('actor', 'login_status', 'open_login', 'translate', 'series_translate', 'series_report'):
                state['phase'] = 'direct'
                service = 'dependency' if kind in ('translate', 'series_translate', 'series_report') else 'browser'
                args = dict(payload, code=task['code'])
                if kind == 'series_report':
                    with self.store.connect() as conn:
                        if payload.get('parent_id'):
                            parent = series_catalog.completed_report(conn, payload['parent_id'])
                            series_catalog.validate_review_groups(conn, parent, payload['draft_groups'])
                            args['members'] = parent['members']
                        else:
                            args['members'] = series_catalog.report_snapshot(conn)
                if kind == 'translate' and task['code']:
                    args['snapshot'] = snapshot(self.store, task['code'])
                if kind == 'actor' and payload.get('actor_id'):
                    with self.store.connect() as conn:
                        row = conn.execute('SELECT * FROM actors WHERE id=? AND COALESCE(is_ignored,0)=0', (payload['actor_id'],)).fetchone()
                    if row is None:
                        raise TargetChanged('演员不存在或已忽略')
                    args['actor_snapshot'] = dict(row)
                self.request(task, state, service, kind, args)
                return
            else:
                raise ValueError('未知工作流类型 ' + kind)
            state['phase'] = 'scrape'
        if state['phase'] == 'scrape':
            expected = snapshot(self.store, state['code'])
            if 'response' not in state:
                self.request(task, state, 'browser', 'video', {'code': state['code'], 'snapshot': expected})
                return
            result = self.response(state)
            # Metadata writes can be replayed after a crash, using the same durable result.
            state['phase'] = 'assets'
            state['metadata_result'] = result
            new_actors = []
            def apply_metadata(conn):
                new_actors.extend(workflows.update_video_metadata(
                    expected[0]['id'], 'FREEZE' if result == 'FREEZE' else 'SUCCESS' if result else 'FAILED',
                    result if isinstance(result, dict) else None, connection=conn))
                if isinstance(result, dict):
                    series_catalog.attach(conn, self.store, state['code'], result.get('series'))
                for actor_id, name in new_actors:
                    self.store.submit_actor(name, actor_id, source_code=state['code'], connection=conn)
            self.commit_phase(task, state, apply_metadata)
            self.emit(task, 'scrape', 'done', '刮削结果已保存')
            self.emit(task, 'actors', 'done', f'已提交 {len(new_actors)} 个新演员刮削任务' if new_actors else '演员关联已更新')
            return
        if state['phase'] == 'assets':
            result = state.get('metadata_result')
            if isinstance(result, dict) and 'response' not in state:
                self.request(task, state, 'dependency', 'metadata_assets', {'code': state['code'], 'data': result,
                    'snapshot': snapshot(self.store, state['code'])})
                return
            if 'response' in state:
                response = state.pop('response')
                if response['outcome'] == 'DONE' and response['result']:
                    state['asset_result'] = response['result']
            if kind == 'pipeline_rescrape' and not isinstance(result, dict):
                self.next_item(task, state, False, '未找到精确匹配，跳过后处理')
                return
            state['phase'] = 'complete' if kind == 'video' else 'translate'
            if kind == 'video':
                state['result'] = result
            def apply_cover(conn):
                assets = state.get('asset_result', {})
                if assets.get('cover_path'):
                    conn.execute('UPDATE videos SET cover_path=? WHERE code=? AND deleted=0', (assets['cover_path'], state['code']))
            self.commit_phase(task, state, apply_cover)
            return
        if state['phase'] == 'translate':
            video = workflows.get_video_by_code(state['code']) or {}
            if 'response' not in state and video.get('title_jp'):
                actors = video.get('actors') or []
                args = {'title': video['title_jp'], 'codes': [state['code']], 'code': state['code'],
                        'actor_name_map': {a['name']: a.get('name_zh') or a['name'] for a in actors},
                        'snapshot': snapshot(self.store, state['code'])}
                self.request(task, state, 'dependency', 'translate', args)
                return
            response = state.pop('response', {})
            title = response.get('result') if response.get('outcome') == 'DONE' else ''
            self.emit(task, 'translate', 'done', title or response.get('error') or '跳过翻译')
            state['translated_title'] = title
            state['phase'] = 'short_title'
            def apply_translation(conn):
                if title:
                    conn.execute('UPDATE videos SET title_zh=? WHERE code=? AND deleted=0', (title, state['code']))
            self.commit_phase(task, state, apply_translation)
            return
        if state['phase'] == 'short_title':
            if 'response' not in state and state.get('translated_title'):
                video = workflows.get_video_by_code(state['code']) or {}
                self.request(task, state, 'dependency', 'short_title', {'title': state['translated_title'],
                    'actors': [a.get('name_zh') or a['name'] for a in video.get('actors', [])],
                    'code': state['code'], 'snapshot': snapshot(self.store, state['code'])})
                return
            response = state.pop('response', {})
            title = response.get('result') if response.get('outcome') == 'DONE' else ''
            self.emit(task, 'short_title', 'done', title or response.get('error') or '跳过短标题')
            state['phase'] = 'rename'
            def apply_short_title(conn):
                if title:
                    conn.execute('UPDATE videos SET title_zh_short=? WHERE code=? AND deleted=0', (title, state['code']))
            self.commit_phase(task, state, apply_short_title)
            return
        if state['phase'] == 'rename':
            snapshot(self.store, state['code'])
            self.emit(task, 'calc_filename', 'running', '计算文件名…')
            workflows._rename_files_for_code(state['code'], job=job)
            state.update({key: value for key, value in job.checkpoint().items() if key in ('rename_moves', 'rename_identity')})
            self.emit(task, 'calc_filename', 'done', '文件名已计算')
            self.emit(task, 'rename', 'done', '文件与数据库路径已更新')
            self.emit(task, 'db_update', 'done', '数据库已更新')
            if kind == 'pipeline_rescrape':
                self.next_item(task, state, True, '刮削完成')
            else:
                self.finish(task, state)
            return
        if state['phase'] == 'direct':
            result = self.response(state)
            state['result'] = result
            if kind == 'actor' and payload.get('actor_id'):
                if result and result.get('avatar_url'):
                    state['actor_result'] = result
                    state['phase'] = 'actor_asset'
                    with self.store.connect() as conn:
                        actor = conn.execute('SELECT * FROM actors WHERE id=?', (payload['actor_id'],)).fetchone()
                    self.request(task, state, 'dependency', 'avatar', {'url': result['avatar_url'], 'name': task['code'],
                        'actor_snapshot': dict(actor) if actor else None})
                    return
                from dal.db_manager import update_actor_avatar
                state['phase'] = 'complete'
                self.commit_phase(task, state, lambda conn: update_actor_avatar(payload['actor_id'], None,
                    task['code'], (result or {}).get('name_zh'), connection=conn))
                return
            if kind == 'translate' and payload.get('codes'):
                state['phase'] = 'complete'
                def apply_translation(conn):
                    for code in payload['codes']:
                        conn.execute("UPDATE videos SET title_zh=? WHERE code=? AND title_jp=? AND COALESCE(title_zh,'')=? AND deleted=0",
                                     (result, code, payload['title'], payload.get('previous_titles', {}).get(code, '')))
                self.commit_phase(task, state, apply_translation)
                return
            self.finish(task, state)
            return
        if state['phase'] == 'complete':
            self.finish(task, state)
            return
        if state['phase'] == 'actor_asset':
            result = self.response(state)
            from dal.db_manager import update_actor_avatar
            state['phase'] = 'complete'
            self.commit_phase(task, state, lambda conn: update_actor_avatar(payload['actor_id'], result, task['code'],
                state['actor_result'].get('name_zh'), connection=conn))
            return
        raise ValueError('未知工作流阶段 ' + state['phase'])

    def next_item(self, task, state, ok, message):
        self.emit(task, state['code'], 'done' if ok else 'error', message)
        items = state.get('items', []) + [{'code': state['code'], 'ok': ok, 'message': message}]
        self.store.release_targets(task['id'])
        journals = self.store.checkpoint(task['id'])
        self.store.save_phase(task['id'], {'phase': 'start', 'index': state.get('index', 0) + 1, 'items': items,
            'rename_moves': journals.get('rename_moves', {}), 'rename_identity': journals.get('rename_identity', {})})

    def validate_result(self, task):
        state = json.loads(task['checkpoint_json'])
        if 'response' not in state:
            return
        with self.store.connect() as conn:
            command = conn.execute('SELECT payload_json FROM dependency_commands WHERE execution_id=? AND id=?',
                                   (task['execution_id'], state['response']['command_id'])).fetchone()
        if command:
            payload = json.loads(command['payload_json'])
            expected = payload.get('snapshot')
            if expected is not None and not matches_video_snapshot(self.store, payload['code'], expected):
                raise TargetChanged('影片在外部调用期间已改变，结果已保留在历史')

    def recover_uploads(self):
        import os
        with self.store.connect(write=True) as conn:
            for row in conn.execute("SELECT id,owner FROM scrape_tasks WHERE status='PREPARING'").fetchall():
                try:
                    os.kill(int(row['owner']), 0)
                except (ProcessLookupError, TypeError, ValueError):
                    conn.execute("UPDATE scrape_tasks SET status='PENDING',owner=NULL WHERE id=?", (row['id'],))
                except PermissionError:
                    pass

    def tick(self):
        for future, command in list(self.futures.items()):
            if future.done():
                try:
                    self.store.publish(command, 'DONE', future.result())
                except Exception as exc:
                    self.store.publish(command, 'FAILED', error=str(exc))
                del self.futures[future]
        self.recover_uploads()
        if self.store.service('consumer')['paused']:
            return False
        progressed = self.store.consume_result()
        if len(self.futures) < 2:
            command = self.store.claim_command('dependency', self.owner)
            if command is None and not any(job['service'] == 'subtitle' for job in self.futures.values()):
                command = self.store.claim_command('subtitle', self.owner)
            if command:
                self.futures[self.pool.submit(execute_dependency, command)] = command
                progressed = True
        task = self.store.claim_workflow(self.owner)
        if task:
            self.store.service('consumer', current_id=task['id'], state='processing')
            try:
                self.validate_result(task)
                self.advance(task)
            except TargetChanged as exc:
                state = json.loads(task['checkpoint_json'])
                if task['kind'] == 'pipeline_rescrape' and state.get('code'):
                    self.next_item(task, state, False, str(exc))
                else:
                    self.store.finish(task['id'], 'SKIPPED', error=str(exc))
                    self.emit(task, 'error', 'error', str(exc))
            except Exception as exc:
                logging.exception('Workflow %s failed', task['id'])
                if task['kind'] == 'pipeline_rescrape':
                    state = json.loads(task['checkpoint_json'])
                    if state.get('code'):
                        self.next_item(task, state, False, str(exc))
                    else:
                        self.store.finish(task['id'], 'FAILED', error=str(exc))
                else:
                    self.store.finish(task['id'], 'FAILED', error=str(exc))
                    self.emit(task, 'error', 'error', str(exc))
            return True
        return progressed

    def run(self):
        self.store.recover_service('consumer')
        self.store.recover_service('dependency')
        self.store.recover_service('subtitle')
        while not self.terminate.is_set():
            control = self.store.service('consumer')
            if control['desired'] != 'run' or control['restart_token'] != control['generation']:
                break
            if not self.tick():
                self.store.service('consumer', state='paused' if control['paused'] else 'idle', current_id=None,
                                   message='Consumer 已暂停' if control['paused'] else '等待任务')
                self.terminate.wait(0.25)
        self.store.service('consumer', state='stopping', message='等待在途外部调用保存结果')
        self.pool.shutdown(wait=True)
        # Persist completed calls before exiting, even when paused.
        for future, command in self.futures.items():
            try:
                self.store.publish(command, 'DONE', future.result())
            except Exception as exc:
                self.store.publish(command, 'FAILED', error=str(exc))

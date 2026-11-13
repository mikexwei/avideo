"""Local administration of services, workflows, commands and result events."""
import json
import math
import time
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

from flask import Blueprint, jsonify, redirect, request
from core.browser_queue import BROWSER_TIMEZONE
from core import javdb_tag_check
from core.series_catalog import (
    REPORT_VERSION, ReviewConflict, combine_review_groups, completed_report, normalize_input,
    publish as publish_series, report_units, validate_review_groups,
)
from core.worker_client import ensure_worker, store
from core.service_manager import control_service


def register_admin_api(app):
    api = Blueprint('admin', __name__)

    @api.before_request
    def protect_controls():
        origin = request.headers.get('Origin')
        if request.method != 'GET' and origin and urlparse(origin).netloc != request.host:
            return jsonify(error='跨站控制请求被拒绝'), 403

    @api.errorhandler(ValueError)
    def invalid(error):
        return jsonify(error=str(error)), 400

    @api.get('/admin')
    def page():
        return redirect('/#/admin')

    def tag_check_summary(row):
        state = json.loads(row['checkpoint_json'])
        return dict(id=row['id'], status=row['status'], created_at=row['created_at'],
                    error=row['error'], current_code=state.get('code'),
                    total=len(json.loads(row['payload_json'])['codes']),
                    checked=state.get('index', 0),
                    found=sum(item['status'] == 'found' for item in state.get('items', [])))

    @api.get('/api/tasks')
    def async_tasks():
        limit = min(100, max(1, request.args.get('limit', 20, type=int)))
        offset = max(0, request.args.get('offset', 0, type=int))
        with store().connect() as conn:
            total = conn.execute('SELECT COUNT(*) FROM scrape_tasks WHERE kind=? AND deleted=0',
                                 (javdb_tag_check.KIND,)).fetchone()[0]
            rows = conn.execute("""SELECT * FROM scrape_tasks WHERE kind=? AND deleted=0
                ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?""",
                (javdb_tag_check.KIND, limit, offset)).fetchall()
            active = conn.execute("""SELECT 1 FROM scrape_tasks WHERE kind=? AND deleted=0
                AND cancel_requested=0 AND status IN (?, ?, ?, ?) LIMIT 1""",
                (javdb_tag_check.KIND, *javdb_tag_check.ACTIVE)).fetchone()
        return jsonify(items=[tag_check_summary(row) for row in rows], total=total, active=bool(active))

    @api.post('/api/tasks/javdb-ltx-check')
    def start_tag_check():
        runtime = store()
        with runtime.connect(write=True) as conn:
            active = conn.execute("""SELECT id FROM scrape_tasks WHERE kind=? AND deleted=0
                AND cancel_requested=0 AND status IN (?, ?, ?, ?) LIMIT 1""",
                (javdb_tag_check.KIND, *javdb_tag_check.ACTIVE)).fetchone()
            if active:
                return jsonify(id=active['id']), 202
            codes = javdb_tag_check.candidates(conn)
            task_id = runtime.submit(javdb_tag_check.KIND, payload={'codes': codes}, priority=-100, connection=conn)
        ensure_worker()
        return jsonify(id=task_id), 202

    @api.get('/api/tasks/<task_id>')
    def async_task(task_id):
        runtime = store()
        row = runtime.get(task_id)
        if not row or row['kind'] != javdb_tag_check.KIND or row['deleted']:
            return jsonify(error='任务不存在'), 404
        state = json.loads(row['checkpoint_json'])
        items = state.get('items', [])
        status = request.args.get('status', 'found')
        if status not in ('all', 'found', 'absent', 'not_found', 'failed'):
            raise ValueError('未知结果状态')
        if status != 'all':
            items = [item for item in items if item['status'] == status]
        limit = min(100, max(1, request.args.get('limit', 25, type=int)))
        offset = max(0, request.args.get('offset', 0, type=int))
        reason = None
        if row['status'] in javdb_tag_check.ACTIVE:
            consumer = runtime.service('consumer')
            browser = runtime.service('browser')
            if not consumer['alive'] or consumer['paused']:
                reason = '等待 Consumer 恢复'
            elif not browser['alive']:
                reason = '等待浏览器服务启动'
            elif browser['state'] in ('waiting_login', 'waiting_verification', 'cooling_down', 'hourly_limit'):
                reason = browser['message']
            elif browser['paused']:
                reason = '等待浏览器服务恢复'
        return jsonify(**tag_check_summary(row), items=items[offset:offset + limit],
                       result_total=len(items), wait_reason=reason)

    @api.get('/api/admin/services')
    def services():
        runtime = store()
        browser = runtime.service('browser')
        settings = runtime.settings('browser')
        if not browser['alive'] or settings.get('session_generation') != browser['generation']:
            settings.update(session_open=False, session_logged_in=None)
        return jsonify(services=[runtime.service('consumer'), browser], browser=settings)

    @api.route('/api/admin/series/reports', methods=['GET', 'POST'])
    def series_reports():
        runtime = store()
        if request.method == 'POST':
            with runtime.connect(write=True) as conn:
                active = conn.execute("""SELECT id FROM scrape_tasks WHERE kind='series_report'
                    AND status IN ('PENDING','RUNNING','WAITING') AND deleted=0 AND cancel_requested=0 LIMIT 1""").fetchone()
                task_id = active['id'] if active else runtime.submit(
                    'series_report', '系列聚合报告', priority=-100, connection=conn)
            ensure_worker()
            return jsonify(id=task_id), 202
        with runtime.connect() as conn:
            rows = conn.execute("""SELECT t.id,t.status,t.created_at,t.error,p.published_at,
                json_extract(t.result_json,'$.version') AS version,
                COALESCE(json_extract(t.payload_json,'$.round'),1) AS round
                FROM scrape_tasks t LEFT JOIN series_publications p ON p.report_id=t.id
                WHERE t.kind='series_report' AND t.deleted=0 ORDER BY t.created_at DESC LIMIT 20""").fetchall()
        return jsonify([dict(row) for row in rows])

    @api.get('/api/admin/series/reports/<report_id>')
    def series_report(report_id):
        with store().connect() as conn:
            row = conn.execute("""SELECT t.status,t.result_json,t.error,p.published_at,p.groups_json
                FROM scrape_tasks t LEFT JOIN series_publications p ON p.report_id=t.id
                WHERE t.id=? AND t.kind='series_report' AND t.deleted=0""", (report_id,)).fetchone()
        if not row:
            return jsonify(error='报告不存在'), 404
        result = json.loads(row['result_json'] or 'null')
        if result and result.get('version') == REPORT_VERSION and result.get('round', 1) > 1:
            members = result.pop('members')
            units = report_units(members, result['draft_groups'])
            final_ids = {member['id']: member['id_final'] for member in members if member['id_final'] is not None}
            for unit in units:
                unit['final_ids'] = sorted({final_ids[i] for i in unit['member_ids'] if i in final_ids})
            result['units'] = units
            result['final_names'] = {member['id_final']: member['final_name'] for member in members
                                     if member['id_final'] is not None}
        return jsonify(status=row['status'], error=row['error'], result=result,
                       published_at=row['published_at'], published_groups=json.loads(row['groups_json'] or 'null'))

    @api.post('/api/admin/series/reports/<report_id>/next')
    def series_report_next(report_id):
        data = request.get_json(silent=True)
        groups = data.get('groups') if isinstance(data, dict) else None
        if not isinstance(groups, list) or not groups:
            raise ValueError('请先确认至少一组审核草稿')
        groups = combine_review_groups(groups)
        if any(not normalize_input(group['name']) for group in groups):
            raise ValueError('草稿名称须包含可用于比较的文字或数字')
        runtime = store()
        try:
            with runtime.connect(write=True) as conn:
                parent = completed_report(conn, report_id)
                if conn.execute('SELECT 1 FROM series_publications WHERE report_id=?', (report_id,)).fetchone():
                    raise ReviewConflict('本轮已发布，请生成新报告')
                payload = {'parent_id': report_id, 'draft_groups': groups, 'round': parent.get('round', 1) + 1}
                active = conn.execute("""SELECT id,payload_json FROM scrape_tasks WHERE kind='series_report'
                    AND status IN ('PENDING','RUNNING','WAITING') AND deleted=0 AND cancel_requested=0 LIMIT 1""").fetchone()
                if active and json.loads(active['payload_json']) != payload:
                    raise ReviewConflict('已有系列报告正在生成，请稍后重试')
                validate_review_groups(conn, parent, groups)
                existing = active or conn.execute("""SELECT id FROM scrape_tasks
                    WHERE kind='series_report' AND status='DONE' AND deleted=0 AND payload_json=?
                    ORDER BY created_at DESC LIMIT 1""", (json.dumps(payload, ensure_ascii=False),)).fetchone()
                task_id = existing['id'] if existing else runtime.submit(
                    'series_report', '系列聚合报告', payload, priority=-100, connection=conn)
        except ReviewConflict as exc:
            return jsonify(error=str(exc)), 409
        ensure_worker()
        return jsonify(id=task_id), 202

    @api.post('/api/admin/series/reports/<report_id>/publish')
    def series_report_publish(report_id):
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            raise ValueError('审核结果格式无效')
        try:
            with store().connect(write=True) as conn:
                publish_series(conn, report_id, data.get('groups'))
        except ReviewConflict as exc:
            return jsonify(error=str(exc)), 409
        return jsonify(ok=True)

    @api.get('/api/admin/browser/usage')
    def browser_usage():
        zone = BROWSER_TIMEZONE
        today = datetime.now(zone).date()
        day = date.fromisoformat(request.args.get('date', today.isoformat()))
        if day > today:
            raise ValueError('日期不能晚于今天')
        start = int(datetime.combine(day, datetime.min.time(), zone).timestamp())
        end = int(datetime.combine(day + timedelta(days=1), datetime.min.time(), zone).timestamp())
        now = time.time()
        with store().connect() as conn:
            tracking = conn.execute("SELECT value_json FROM scrape_settings WHERE key='browser_access_since'").fetchone()
            since = json.loads(tracking[0]) if tracking else None
            profiles = [(when, json.loads(profile)) for when, profile in conn.execute("""
                SELECT effective_at,profile_json FROM browser_quota
                WHERE effective_at>=? OR effective_at=(SELECT MAX(effective_at) FROM browser_quota WHERE effective_at<?)
                ORDER BY effective_at DESC""", (min(start, int(now)), min(start, int(now))))]
            counts = {}
            for hour, kind, used in conn.execute("""SELECT CAST((accessed_at - ?) / 3600 AS INTEGER),
                    kind, COUNT(*) FROM browser_access WHERE accessed_at >= ? AND accessed_at < ?
                    GROUP BY 1, kind""", (start, start, min(end, now))):
                counts.setdefault(hour, {})[kind] = used
        hours = []
        for hour, timestamp in enumerate(range(start, end, 3600)):
            kinds = counts.get(hour, {})
            covered = since is not None and max(timestamp, since) < min(timestamp + 3600, now)
            local = datetime.fromtimestamp(timestamp, zone)
            profile = next((profile for when, profile in profiles if when <= timestamp), None)
            hours.append(dict(start=timestamp, label=local.strftime('%H:%M'),
                              allowed=profile['limits'][local.hour] if profile else None,
                              used=sum(kinds.values()) if covered else None, kinds=kinds,
                              partial=covered and since > timestamp))
        effective_at, profile = profiles[0] if profiles else (None, None)
        return jsonify(date=day.isoformat(), today=today.isoformat(), timezone=zone.key, tracking_since=since,
                       quota=profile, effective_at=effective_at, hours=hours)

    @api.put('/api/admin/browser/quota')
    def browser_quota():
        data = request.get_json(silent=True)
        if not isinstance(data, dict) or set(data) != {'peak_hour', 'peak_limit', 'spread'}:
            raise ValueError('请提供峰值小时、峰值额度和曲线宽度')
        peak_hour, peak_limit, spread = (data[key] for key in ('peak_hour', 'peak_limit', 'spread'))
        if (type(peak_hour) is not int or not 0 <= peak_hour <= 23 or
                type(peak_limit) is not int or not 1 <= peak_limit <= 3600 or
                type(spread) not in (int, float) or not 0.5 <= spread <= 12):
            raise ValueError('峰值小时须为 0–23，额度 1–3600 次，曲线宽度 0.5–12 小时')
        distances = [min(abs(hour - peak_hour), 24 - abs(hour - peak_hour)) for hour in range(24)]
        limits = [math.floor(peak_limit * math.exp(-0.5 * (distance / spread) ** 2) + 0.5)
                  for distance in distances]
        profile = dict(peak_hour=peak_hour, peak_limit=peak_limit, spread=spread, limits=limits)
        runtime = store()
        with runtime.connect(write=True) as conn:
            effective_at = (int(time.time() // 3600) + 1) * 3600
            conn.execute('INSERT OR REPLACE INTO browser_quota(effective_at,profile_json) VALUES (?,?)',
                         (effective_at, json.dumps(profile)))
        runtime.audit('browser_quota', 'browser', dict(profile, effective_at=effective_at))
        return jsonify(quota=profile, effective_at=effective_at)

    @api.post('/api/admin/services/<name>/control')
    def control(name):
        if name not in ('consumer', 'browser'):
            raise ValueError('未知服务')
        return jsonify(control_service(name, (request.get_json(silent=True) or {}).get('action')))

    @api.route('/api/admin/browser/settings', methods=['GET', 'PATCH'])
    def settings():
        if request.method == 'PATCH':
            data = request.get_json(silent=True) or {}
            fields = {'interval_min', 'interval_max', 'backoff_base', 'backoff_max'}
            if not data or set(data) - fields:
                raise ValueError('只能修改访问间隔和失败退避参数')
            values = {}
            for key, value in data.items():
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 1 <= value <= 86400:
                    raise ValueError('时间必须为 1–86400 秒之间的有限数值')
                values[key] = value
            with store().connect(write=True) as conn:
                current = json.loads(conn.execute("SELECT value_json FROM scrape_settings WHERE key='browser'").fetchone()[0])
                current.update(values)
                if current['interval_min'] > current['interval_max'] or current['backoff_base'] > current['backoff_max']:
                    raise ValueError('最小值不能超过最大值')
                # Existing cooldown remains in force. New values apply on the next reservation.
                conn.execute("UPDATE scrape_settings SET value_json=? WHERE key='browser'", (json.dumps(current),))
            store().audit('browser_settings', 'browser', values)
        return jsonify(store().settings('browser'))

    @api.post('/api/admin/browser/<action>')
    def browser_action(action):
        if action not in ('login-status', 'open-login'):
            raise ValueError('未知浏览器操作')
        task_id = store().submit('login_status' if action == 'login-status' else 'open_login', forced=True)
        return jsonify(job_id=task_id), 202

    @api.get('/api/admin/<resource>')
    def listing(resource):
        tables = {'workflows': 'scrape_tasks', 'commands': 'dependency_commands', 'events': 'dependency_results', 'history': 'scrape_history', 'audit': 'admin_history'}
        if resource not in tables:
            raise ValueError('未知列表')
        limit = min(max(request.args.get('limit', 50, type=int), 1), 200)
        offset = max(request.args.get('offset', 0, type=int), 0)
        filters, args = [], []
        status = request.args.get('status', '')
        task_id = request.args.get('task_id', '')
        if status and resource in ('workflows', 'commands'):
            filters.append('status=?')
            args.append(status)
        if task_id:
            if resource == 'workflows':
                filters.append('id=?')
            elif resource in ('commands', 'history'):
                filters.append('task_id=?')
            elif resource == 'events':
                filters.append('command_id IN (SELECT id FROM dependency_commands WHERE task_id=?)')
            else:
                raise ValueError('该列表不支持任务筛选')
            args.append(task_id)
        where = ' WHERE ' + ' AND '.join(filters) if filters else ''
        table = tables[resource]
        order = 'created_at DESC,id DESC' if resource in ('workflows', 'commands') else 'id DESC'
        with store().connect() as conn:
            total = conn.execute('SELECT COUNT(*) FROM ' + table + where, args).fetchone()[0]
            rows = [dict(row) for row in conn.execute('SELECT * FROM ' + table + where + ' ORDER BY ' + order + ' LIMIT ? OFFSET ?', [*args, limit, offset])]
        return jsonify(items=rows, total=total, offset=offset, limit=limit)

    @api.get('/api/admin/workflows/<task_id>')
    @api.get('/api/jobs/<task_id>')
    def workflow(task_id):
        task = store().get(task_id)
        if task is None:
            return jsonify(error='任务不存在'), 404
        task['result'] = json.loads(task['result_json']) if task['result_json'] else None
        return jsonify(task)

    @api.post('/api/admin/workflows/<task_id>/<action>')
    def task_action(task_id, action):
        task = store().get(task_id)
        if not task or task['kind'] in ('ltx', 'ltx_preview'):
            raise ValueError('任务不存在或功能已关闭')
        if action == 'cancel':
            store().cancel_task(task_id)
        elif action == 'retry':
            task_id = store().retry_task(task_id)
        elif action == 'priority':
            priority = (request.get_json(silent=True) or {}).get('priority')
            if isinstance(priority, bool) or not isinstance(priority, int) or not -10000 <= priority <= 10000:
                raise ValueError('优先级必须为 -10000 至 10000 的整数')
            with store().connect(write=True) as conn:
                conn.execute('UPDATE scrape_tasks SET priority=? WHERE id=?', (priority, task_id))
                conn.execute("UPDATE dependency_commands SET priority=? WHERE task_id=? AND status='PENDING'",
                             (100000 + priority if task['forced'] else priority, task_id))
            store().audit('priority', task_id, {'priority': priority})
        else:
            raise ValueError('未知工作流操作')
        return jsonify(ok=True, job_id=task_id)

    @api.post('/api/admin/events/<int:event_id>/redeliver')
    def redeliver(event_id):
        with store().connect() as conn:
            row = conn.execute("""SELECT r.*,c.task_id,c.status AS command_status,c.attempt AS current_attempt FROM dependency_results r
                JOIN dependency_commands c ON c.id=r.command_id WHERE r.id=?""", (event_id,)).fetchone()
        if not row:
            raise ValueError('事件不存在')
        if row['acknowledged_at'] is None:
            store().audit('redeliver', str(event_id))
            return jsonify(ok=True, message='事件等待 Consumer 自动消费')
        task = store().get(row['task_id'])
        if row['outcome'] not in ('FAILED', 'SKIPPED') or row['command_status'] != row['outcome'] or row['current_attempt'] != row['attempt'] or task['execution_id'] != row['execution_id']:
            raise ValueError('该事件不可重投')
        if task['status'] in ('FAILED', 'SKIPPED', 'CANCELLED'):
            store().retry_task(task['id'])
            job_id = task['id']
        elif task['kind'] == 'pipeline_rescrape' and task['status'] == 'DONE':
            with store().connect() as conn:
                command = conn.execute('SELECT payload_json FROM dependency_commands WHERE id=?', (row['command_id'],)).fetchone()
            code = json.loads(command['payload_json']).get('code')
            if not code:
                raise ValueError('失败事件没有可重试的影片')
            job_id = store().submit('pipeline_rescrape', code, {**json.loads(task['payload_json']), 'codes': [code]})
        else:
            raise ValueError('工作流尚未结束，或该阶段已经成功')
        store().audit('redeliver', str(event_id), {'job_id': job_id})
        return jsonify(ok=True, job_id=job_id), 202

    app.register_blueprint(api)

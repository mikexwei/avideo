import json
import re
from urllib.parse import urlparse

from flask import Blueprint, jsonify, request

from core.ltx_scheduler import quota
from core.scrape_queue import QueueConflict, TERMINAL, base_code
from core.worker_client import WorkerClient, control_worker, ensure_worker, store


def register_scrape_api(app, candidates):
    api = Blueprint('scrape_queue', __name__)

    @api.before_request
    def protect_controls():
        origin = request.headers.get('Origin')
        if request.method != 'GET' and origin and urlparse(origin).netloc != request.host:
            return jsonify(error='跨站控制请求被拒绝'), 403

    def codes_from_request():
        data = request.get_json(silent=True) or {}
        codes = data.get('codes')
        if not isinstance(codes, list) or not codes or len(codes) > 5000 or any(
            not isinstance(code, str) or not re.fullmatch(r'[A-Z0-9]+(?:-[A-Z0-9]+)+', code.strip().upper())
            for code in codes
        ):
            raise ValueError('codes 必须为非空番号列表，最多 5000 个')
        return data, list(dict.fromkeys(base_code(code) for code in codes))

    @api.errorhandler(ValueError)
    def invalid(error):
        if isinstance(error, QueueConflict):
            return jsonify(error=str(error), confirm_required=True, conflicts=error.rows), 409
        return jsonify(error=str(error)), 400

    def catalog():
        with store().connect() as conn:
            rows = conn.execute('''SELECT
                UPPER(CASE WHEN UPPER(v.code) LIKE '%-UC' THEN SUBSTR(v.code,1,LENGTH(v.code)-3)
                    WHEN UPPER(v.code) LIKE '%-U' OR UPPER(v.code) LIKE '%-C' THEN SUBSTR(v.code,1,LENGTH(v.code)-2)
                    ELSE v.code END) AS code,
                MAX(COALESCE(v.title_zh,v.title_jp,'')) AS title,MAX(v.score) AS score,
                COALESCE(GROUP_CONCAT(DISTINCT CASE WHEN COALESCE(a.is_ignored,0)=0 THEN COALESCE(a.name_zh,a.name) END),'') AS actresses
                FROM videos v LEFT JOIN video_actor_link l ON l.video_id=v.id LEFT JOIN actors a ON a.id=l.actor_id
                WHERE v.deleted=0 GROUP BY 1''').fetchall()
            downloaded = {}
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ltx_scrape_results'").fetchone():
                downloaded = {row['code']: bool(row['downloaded']) for row in conn.execute('SELECT code,downloaded FROM ltx_scrape_results')}
        result = {row['code']: dict(row) for row in rows}
        for code, item in result.items():
            item['downloaded'] = downloaded.get(code, False)
        return result

    def task_view(task, metadata):
        item = dict(metadata.get(task['code'], {'code': task['code'], 'title': '', 'actresses': '', 'score': None}))
        item.update(id=task['id'], queue_status=task['status'], status=task['result_status'],
                    scraped=task['status'] in TERMINAL, priority=task['priority'], attempts=task['attempts'],
                    error=task['error'], links=json.loads(task['result_json'] or '[]'), forced=bool(task['forced']),
                    queued_at=task['queued_at'], finished_at=task['finished_at'], next_run_at=task['next_run_at'])
        item['downloaded'] = bool(item.get('downloaded'))
        return item

    @api.get('/api/ltx/queue')
    def queue_list():
        metadata = catalog()
        return jsonify([task_view(task, metadata) for task in store().list_tasks()])

    @api.get('/api/ltx/results')
    def results():
        tasks = {task['code']: task for task in store().list_tasks()}
        rows = candidates()
        for row in rows:
            task = tasks.get(row['code'])
            if task:
                row.update(task_view(task, {row['code']: row}))
            else:
                row.update(id=None, queue_status=None, priority=0, attempts=0)
        return jsonify(rows)

    @api.get('/api/ltx/status')
    def status():
        queue = store()
        tasks = queue.list_tasks()
        worker, control = queue.worker_state(), queue.settings('control')
        pending = [task['code'] for task in tasks if task['status'] in ('PENDING', 'RUNNING')]
        state = worker['state'] if worker['alive'] else 'stopped'
        return jsonify(worker=worker, control=control, quota=quota(queue.path),
                       task={'state': state, 'message': worker['message'], 'code': worker['code'],
                             'active': worker['alive'] and not control['paused'] and control['desired'] == 'run',
                             'current': len(tasks) - len(pending), 'total': len(tasks), 'remaining': len(pending),
                             'queued_codes': pending, 'next_at': worker['next_at'],
                             'found': sum(task['result_status'] == 'FOUND' for task in tasks),
                             'errors': sum(task['status'] == 'FAILED' for task in tasks)})

    @api.post('/api/worker/control')
    def worker_control():
        return jsonify(worker=control_worker((request.get_json(silent=True) or {}).get('action')))

    @api.route('/api/worker/quota', methods=['GET', 'PATCH'])
    def worker_quota():
        if request.method == 'PATCH':
            data = request.get_json(silent=True) or {}
            try:
                values = {'daily_limit': int(data['daily_limit']), 'peak_hour': int(data['peak_hour']), 'spread': float(data['spread'])}
            except (KeyError, TypeError, ValueError):
                raise ValueError('请输入每日上限、峰值小时和分布宽度')
            if not 1 <= values['daily_limit'] <= 10000 or not 0 <= values['peak_hour'] <= 23 or not 0.5 <= values['spread'] <= 12:
                raise ValueError('配额范围：每日 1–10000，峰值 0–23，宽度 0.5–12')
            store().set_settings('quota', values)
            with store().connect(write=True) as conn:
                conn.execute("UPDATE scrape_tasks SET next_run_at=0 WHERE kind='ltx' AND status='PENDING' AND retries=0")
        return jsonify(quota(store().path))

    @api.post('/api/ltx/scrape/enqueue')
    @api.post('/api/ltx/scrape/start')
    def enqueue():
        data = request.get_json(silent=True) or {}
        if request.path.endswith('/start') and 'codes' not in data:
            codes = [row['code'] for row in candidates()]
        else:
            data, codes = codes_from_request()
        forced = data.get('immediate') is True
        valid, rejected = [], []
        for code in codes:
            reason = store().eligibility(code, forced)
            if reason:
                rejected.append({'code': code, 'reason': reason})
            else:
                valid.append(code)
        result = store().enqueue(valid, confirm=data.get('confirm_rescrape') is True, forced=forced)
        if forced:
            store().set_settings('control', {'desired': 'run'})
        ensure_worker()
        return jsonify(**result, rejected=rejected, total=len(store().list_tasks()))

    @api.post('/api/ltx/queue/bulk')
    def bulk():
        data, codes = codes_from_request()
        action = data.get('action')
        if action not in ('requeue', 'cancel', 'delete', 'priority', 'immediate'):
            raise ValueError('未知队列操作')
        priority = data.get('priority', 0) if action == 'priority' else 0
        if not isinstance(priority, int) or isinstance(priority, bool) or not -10000 <= priority <= 10000:
            raise ValueError('优先级必须为 -10000 到 10000 的整数')
        tasks = [task for task in store().list_tasks() if task['code'] in set(codes)]
        if action in ('requeue', 'immediate') and data.get('confirm_rescrape') is not True:
            completed = [task for task in tasks if task['status'] in TERMINAL]
            if completed:
                raise QueueConflict([{'code': task['code'], 'status': task['status']} for task in completed])
        changed = store().bulk([task['id'] for task in tasks], action, priority)
        if action == 'immediate':
            store().set_settings('control', {'desired': 'run'})
            ensure_worker()
        return jsonify(changed=changed)

    @api.get('/api/ltx/queue/<task_id>/history')
    def history(task_id):
        return jsonify(store().history(task_id))

    @api.post('/api/ltx/scrape/stop')
    def stop():
        control_worker('pause')
        return status()

    @api.get('/api/ltx/login-status')
    def login_status():
        return jsonify(logged_in=WorkerClient().check_login())

    @api.post('/api/ltx/open-login')
    def open_login():
        WorkerClient().open_login()
        return jsonify(ok=True)

    @api.post('/api/ltx/scrape/dry-run')
    def dry_run():
        code = (request.get_json(silent=True) or {}).get('code')
        if not isinstance(code, str) or not code.strip():
            raise ValueError('code 必须为非空番号')
        code = base_code(code)
        result = WorkerClient().scrape_ltx(code)
        return jsonify(code=code, dry_run=True, links=result, status='FOUND' if result else 'NOT_FOUND')

    app.register_blueprint(api)

"""Scrape progress and worker endpoints."""
import json
import time
from flask import Blueprint, jsonify, request
from urllib.parse import urlparse
from core.worker_client import control_worker, store


def register_scrape_api(app):
    api = Blueprint('scrape_queue', __name__)

    @api.before_request
    def protect_controls():
        origin = request.headers.get('Origin')
        if request.method != 'GET' and origin and urlparse(origin).netloc != request.host:
            return jsonify(error='跨站控制请求被拒绝'), 403

    @api.errorhandler(ValueError)
    def invalid(error):
        return jsonify(error=str(error)), 400

    @api.route('/api/ltx', methods=['GET', 'POST', 'PATCH', 'DELETE'])
    @api.route('/api/ltx/<path:path>', methods=['GET', 'POST', 'PATCH', 'DELETE'])
    def disabled(path=''):
        return jsonify(error='LTX 功能已关闭', enabled=False), 410

    @api.get('/api/scrape-progress')
    def progress():
        with store().connect() as conn:
            tasks = conn.execute("""SELECT id, kind, code, status, created_at, error, payload_json, checkpoint_json
                FROM scrape_tasks
                WHERE kind IN ('pipeline_auto', 'pipeline_rescrape', 'video') AND deleted=0 AND created_at>=?
                ORDER BY created_at DESC, id DESC""", (time.time() - 24 * 3600,)).fetchall()
            cards = []
            for task in tasks:
                payload = json.loads(task['payload_json'])
                checkpoint = json.loads(task['checkpoint_json'])
                code = task['code'] or checkpoint.get('code') or payload.get('main_code') or ''
                batch = task['kind'] == 'pipeline_rescrape'
                codes = (payload.get('codes') or [code]) if batch else [code]
                completed = {item['code']: item for item in checkpoint.get('items', [])}
                filenames = [path.rsplit('/', 1)[-1] for path in payload.get('file_paths', [])]
                for index, code in enumerate(codes):
                    status, phase, message = task['status'], checkpoint.get('phase', ''), task['error']
                    if code in completed:
                        item = completed[code]
                        status, phase, message = ('DONE' if item['ok'] else 'FAILED'), '', item.get('message')
                    elif batch and index > checkpoint.get('index', 0) and status in ('PENDING', 'WAITING', 'RUNNING'):
                        status, phase, message = 'PENDING', '', None
                    elif status == 'DONE' and 'metadata_result' in checkpoint:
                        if checkpoint['metadata_result'] == 'FREEZE':
                            status, message = 'SKIPPED', '未找到精确匹配'
                        elif not checkpoint['metadata_result']:
                            status, message = 'FAILED', '未获取到影片资料'
                    cards.append(dict(id=task['id'], kind=task['kind'], code=code, status=status,
                                      phase=phase, message=message, created_at=task['created_at'], files=filenames))
            codes = json.dumps(list({card['code'] for card in cards if card['code']}))
            videos = {}
            for row in conn.execute("""SELECT code, title_zh, title_jp, cover_path FROM videos
                WHERE deleted=0 AND code IN (SELECT value FROM json_each(?)) ORDER BY id""", (codes,)):
                videos.setdefault(row['code'], dict(row))
            for card in cards:
                card['video'] = videos.get(card['code'])
        return jsonify(cards)

    @api.get('/api/worker/status')
    def status():
        return jsonify(worker=store().service('consumer'))

    @api.post('/api/worker/control')
    def control():
        from flask import request
        return jsonify(worker=control_worker((request.get_json(silent=True) or {}).get('action')))

    app.register_blueprint(api)

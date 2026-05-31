import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from flask import Flask, Response, jsonify, request, send_file, send_from_directory
from werkzeug.utils import secure_filename

from config import DB_PATH, MEDIA_LIBRARIES
from core.scanner import extract_video_code
from core.translator.service import shorten_chinese_title_with_candidates, translate_title
from utils.rename_and_move_videos import (
    VideoRecord,
    build_destination,
    build_filename,
)

from dal.db_manager import (
    get_actor_with_videos,
    get_tag_with_videos,
    get_video_by_code,
    get_videos_by_prefix,
    get_videos_by_series,
    get_stats,
    get_recommendations,
    list_all_actors_with_count,
    list_all_prefixes_with_count,
    list_all_series_with_count,
    list_all_tags_with_count,
    list_videos,
    assign_series_cluster,
    delete_video,
    find_actor_by_name,
    get_video_file_path,
    merge_actors,
    patch_actor,
    patch_actor_avatar,
    patch_video,
    patch_video_cover,
    patch_video_file_path,
    patch_video_relations,
    rename_video_code,
    replace_video_file,
    search_actors,
    search_series,
    search_videos,
    batch_insert_scanned_videos,
    sync_derived_video_tags_for_code,
)

app = Flask(__name__, static_folder='../static', static_url_path='/static', template_folder='../templates')
app.json.ensure_ascii = False
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024 * 1024  # 50 GB

# ── Job management ──────────────────────────────────────────────

# Alias avoids shadowing the `queue` module inside the _Job dataclass field annotation
_Queue = queue.Queue


@dataclass
class _Job:
    job_id: str
    queue: _Queue = field(default_factory=_Queue)
    status: str = 'running'
    created_at: float = field(default_factory=time.time)


_jobs: dict = {}
_jobs_lock = threading.Lock()


def _cleanup_old_jobs():
    while True:
        time.sleep(600)
        cutoff = time.time() - 3600
        with _jobs_lock:
            stale = [jid for jid, j in _jobs.items() if j.created_at < cutoff]
            for jid in stale:
                del _jobs[jid]


threading.Thread(target=_cleanup_old_jobs, daemon=True).start()


def _scrape_with_playwright(base_code: str) -> dict:
    """Run Playwright scrape in the calling thread. Returns result_data dict or None."""
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth
    from core.scraper import scrape_video_info, load_javdb_cookies
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, args=['--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(viewport={'width': 1280, 'height': 800})
        load_javdb_cookies(context)
        page = context.new_page()
        Stealth().apply_stealth_sync(page)
        try:
            return scrape_video_info(page, base_code)
        finally:
            browser.close()


def _run_auto_pipeline(job: '_Job', file_paths: list, volume_path: 'Path'):
    def emit(step: str, status: str, message: str, **extra):
        job.queue.put({'step': step, 'status': status, 'message': message, **extra})

    try:
        # ── Step 1: Parse codes ──────────────────────────────────
        sorted_paths = sorted(file_paths, key=lambda p: p.name)
        codes_parts = []
        for i, fpath in enumerate(sorted_paths):
            code, part = extract_video_code(fpath.stem)
            if not code:
                raise ValueError(f'无法解析番号: {fpath.name}，请改用手动模式')
            if len(sorted_paths) > 1 and not part:
                part = f'part{i + 1}'
            codes_parts.append((code, part, fpath))
        main_code = codes_parts[0][0]
        emit('parse_code', 'done', f'番号: {main_code}')

        # ── Steps 2-3: Move to destination ──────────────────────
        dest_dir = None
        scanned = []
        for code, part, tmp_path in codes_parts:
            record = VideoRecord(
                id=0, code=code, part=part, original_file_path=tmp_path,
                title_zh_short=None, title_zh=None, title_jp=None, actors=[],
            )
            dest_path = build_destination(record, dest_root=volume_path)
            dest_dir = dest_path.parent
            dest_dir.mkdir(parents=True, exist_ok=True)
            tmp_path.rename(dest_path)
            st = dest_path.stat()
            scanned.append({
                'code': code, 'part': part, 'original_path': dest_path,
                'file_size': st.st_size,
                'file_mtime': datetime.fromtimestamp(st.st_mtime).isoformat(),
                'file_birthtime': datetime.fromtimestamp(
                    getattr(st, 'st_birthtime', st.st_mtime)
                ).isoformat(),
            })
        emit('calc_dir', 'done', f'目标目录: {dest_dir}')
        emit('upload', 'done', '文件已就位')

        # ── Steps 4-5: Insert DB + sync derived tags ─────────────
        batch_insert_scanned_videos(scanned)
        sync_derived_video_tags_for_code(main_code)
        suffix_upper = main_code.upper()
        if suffix_upper.endswith('-UC'):
            tag_msg = '标签: 中文字幕、无码破解'
        elif suffix_upper.endswith('-C'):
            tag_msg = '标签: 中文字幕'
        elif suffix_upper.endswith('-U'):
            tag_msg = '标签: 无码破解'
        else:
            tag_msg = '无特殊标签'
        emit('db_insert', 'done', f'已写入数据库，{tag_msg}')
        emit('tags', 'done', '标签同步完成')

        # ── Step 6: Scrape ───────────────────────────────────────
        emit('scrape', 'running', '刮削影片信息（JavDB）…')
        base_code = re.sub(r'(?i)-(?:uc|u|c)$', '', main_code)
        result_data = _scrape_with_playwright(base_code)

        if result_data:
            conn_tmp = sqlite3.connect(DB_PATH)
            try:
                cur = conn_tmp.cursor()
                cur.execute('SELECT id FROM videos WHERE code = ? LIMIT 1', (main_code,))
                row = cur.fetchone()
            finally:
                conn_tmp.close()
            if row:
                from dal.db_manager import update_video_metadata
                update_video_metadata(row[0], 'SUCCESS', result_data)
            emit('scrape', 'done', '刮削完成')
            emit('actors', 'done', '演员信息已更新')
        else:
            emit('scrape', 'done', '刮削未找到结果，跳过')
            emit('actors', 'done', '跳过')

        # ── Step 8: Translate ────────────────────────────────────
        emit('translate', 'running', '翻译标题…')
        video_data = get_video_by_code(main_code) or {}
        title_jp = video_data.get('title_jp') or ''
        actor_list = video_data.get('actors') or []
        actor_name_map = {a['name']: (a.get('name_zh') or a['name']) for a in actor_list}
        actor_names = [a.get('name_zh') or a['name'] for a in actor_list]
        title_zh = ''
        if title_jp:
            try:
                title_zh = translate_title(title_jp, [main_code], actor_name_map=actor_name_map)
                patch_video(main_code, {'title_zh': title_zh})
            except Exception:
                logging.exception(f'[{main_code}] 翻译标题失败')
        emit('translate', 'done', f'翻译: {title_zh[:30]}' if title_zh else '无日文标题，跳过')

        # ── Step 9: Short title ──────────────────────────────────
        emit('short_title', 'running', '计算短标题…')
        title_zh_short = ''
        if title_zh:
            try:
                title_zh_short, _ = shorten_chinese_title_with_candidates(
                    title_zh, actor_names=actor_names
                )
                if title_zh_short:
                    patch_video(main_code, {'title_zh_short': title_zh_short})
            except Exception:
                logging.exception(f'[{main_code}] 计算短标题失败')
        emit('short_title', 'done', f'短标题: {title_zh_short}' if title_zh_short else '跳过')

        # ── Steps 10-12: Rename files ────────────────────────────
        emit('calc_filename', 'running', '计算文件名…')
        final_conn = sqlite3.connect(DB_PATH)
        final_conn.row_factory = sqlite3.Row
        try:
            cur = final_conn.cursor()
            cur.execute(
                'SELECT id, code, part, original_file_path, title_zh_short, title_zh, title_jp '
                'FROM videos WHERE UPPER(code) = UPPER(?) AND deleted = 0',
                (main_code,),
            )
            video_rows = cur.fetchall()
            for vrow in video_rows:
                cur.execute(
                    'SELECT COALESCE(a.name_zh, a.name) AS name FROM actors a '
                    'JOIN video_actor_link val ON val.actor_id = a.id '
                    'WHERE val.video_id = ? ORDER BY a.id ASC',
                    (vrow['id'],),
                )
                actors_for_rec = [r['name'] for r in cur.fetchall() if r['name']]
                rec = VideoRecord(
                    id=vrow['id'], code=vrow['code'], part=vrow['part'],
                    original_file_path=Path(vrow['original_file_path']),
                    title_zh_short=vrow['title_zh_short'],
                    title_zh=vrow['title_zh'],
                    title_jp=vrow['title_jp'],
                    actors=actors_for_rec,
                )
                new_name = build_filename(rec)
                old_path = Path(vrow['original_file_path'])
                new_path = old_path.parent / new_name
                if old_path != new_path and old_path.exists():
                    old_path.rename(new_path)
                    cur.execute(
                        'UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP '
                        'WHERE id = ?',
                        (str(new_path), vrow['id']),
                    )
            final_conn.commit()
        finally:
            final_conn.close()

        emit('calc_filename', 'done', '文件名已计算')
        emit('rename', 'done', '文件已重命名')
        emit('db_update', 'done', '数据库已更新')
        emit('done', 'done', '全部完成', code=main_code)

    except Exception as exc:
        emit('error', 'error', str(exc))
    finally:
        job.status = 'done'
        if file_paths:
            try:
                tmp_parent = file_paths[0].parent
                if '.upload_tmp' in str(tmp_parent):
                    import shutil as _shutil
                    _shutil.rmtree(str(tmp_parent), ignore_errors=True)
            except Exception:
                pass


def _selected_tags():
    tags = request.args.getlist('tag')
    tags.extend((request.args.get('tags') or '').split(','))
    return [t.strip() for t in tags if t and t.strip()]


def _compute_final_filename(
    dest_name: str,
    subtitle_zh: bool,
    uncensored_leaked: bool,
    uncensored_native: bool,
    part: Optional[str],
) -> str:
    p = Path(dest_name)
    stem, ext = p.stem, p.suffix
    has_c = subtitle_zh
    has_u = uncensored_leaked or uncensored_native
    if has_c and has_u:
        suffix = '-UC'
    elif has_c:
        suffix = '-C'
    elif has_u:
        suffix = '-U'
    else:
        suffix = ''
    part_seg = f'.part{part}' if part else ''
    return f'{stem}{suffix}{part_seg}{ext}'


@app.get('/api/config/media-libraries')
def api_media_libraries():
    return jsonify([str(p) for p in MEDIA_LIBRARIES])


@app.get('/api/videos/max-code/<string:code_type>')
def api_max_code(code_type: str):
    ctype = code_type.upper()
    if ctype not in ('WEST', 'CN', 'DUMMY'):
        return jsonify({'error': 'invalid code_type'}), 400
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT code FROM videos WHERE UPPER(code) LIKE ?",
            (f'{ctype}-%',),
        )
        rows = cursor.fetchall()
    finally:
        conn.close()
    max_num = 0
    for (code,) in rows:
        try:
            num = int(code.split('-', 1)[1])
            if num > max_num:
                max_num = num
        except (ValueError, IndexError):
            continue
    return jsonify({'code_type': ctype, 'max_number': max_num, 'suggested': max_num + 1})


@app.post('/api/videos/add-manual')
def api_add_manual():
    volume_path_str = (request.form.get('volume_path') or '').strip()
    volume_path = Path(volume_path_str)
    if volume_path not in [Path(p) for p in MEDIA_LIBRARIES]:
        return jsonify({'error': 'invalid volume_path'}), 400

    code_type = (request.form.get('code_type') or '').strip().upper()
    if code_type not in ('WEST', 'CN', 'DUMMY'):
        return jsonify({'error': 'invalid code_type'}), 400

    try:
        code_number = int(request.form.get('code_number') or 0)
    except ValueError:
        return jsonify({'error': 'invalid code_number'}), 400
    if code_number < 1:
        return jsonify({'error': 'code_number must be >= 1'}), 400

    is_multipart = request.form.get('is_multipart') == '1'
    files = request.files.getlist('files[]')
    if not files:
        return jsonify({'error': 'no files'}), 400

    code = f'{code_type}-{code_number:03d}'
    job_id = str(uuid.uuid4())
    tmp_dir = volume_path / '.upload_tmp' / job_id
    tmp_dir.mkdir(parents=True, exist_ok=True)

    sorted_files = sorted(files, key=lambda f: secure_filename(f.filename) or '')
    scanned = []
    try:
        for i, f in enumerate(sorted_files):
            safe_name = secure_filename(f.filename) or f'file_{i}.bin'
            tmp_path = tmp_dir / safe_name
            f.save(str(tmp_path))

            part = f'part{i + 1}' if is_multipart else None
            record = VideoRecord(
                id=0, code=code, part=part,
                original_file_path=tmp_path,
                title_zh_short=None, title_zh=None, title_jp=None, actors=[],
            )
            dest_path = build_destination(record, dest_root=volume_path)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.rename(dest_path)

            st = dest_path.stat()
            scanned.append({
                'code': code,
                'part': part,
                'original_path': dest_path,
                'file_size': st.st_size,
                'file_mtime': datetime.fromtimestamp(st.st_mtime).isoformat(),
                'file_birthtime': datetime.fromtimestamp(
                    getattr(st, 'st_birthtime', st.st_mtime)
                ).isoformat(),
            })

        batch_insert_scanned_videos(scanned)
        sync_derived_video_tags_for_code(code)
    except Exception as exc:
        import shutil as _shutil
        _shutil.rmtree(str(tmp_dir), ignore_errors=True)
        return jsonify({'error': str(exc)}), 500

    try:
        tmp_dir.rmdir()
    except OSError:
        pass

    return jsonify({'codes': [code]})


@app.post('/api/videos/<string:code>/finalize-rename')
def api_finalize_rename(code: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, code, part, original_file_path, title_zh_short, title_zh, title_jp "
            "FROM videos WHERE UPPER(code) = UPPER(?) AND deleted = 0",
            (code,),
        )
        rows = cursor.fetchall()
        if not rows:
            return jsonify({'error': 'not found'}), 404

        results = []
        for row in rows:
            cursor.execute(
                "SELECT COALESCE(a.name_zh, a.name) AS name "
                "FROM actors a JOIN video_actor_link val ON val.actor_id = a.id "
                "WHERE val.video_id = ? ORDER BY a.id ASC",
                (row['id'],),
            )
            actors = [r['name'] for r in cursor.fetchall() if r['name']]
            record = VideoRecord(
                id=row['id'], code=row['code'], part=row['part'],
                original_file_path=Path(row['original_file_path']),
                title_zh_short=row['title_zh_short'],
                title_zh=row['title_zh'],
                title_jp=row['title_jp'],
                actors=actors,
            )
            new_name = build_filename(record)
            old_path = Path(row['original_file_path'])
            new_path = old_path.parent / new_name
            if old_path != new_path and old_path.exists():
                old_path.rename(new_path)
                cursor.execute(
                    "UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP "
                    "WHERE id = ?",
                    (str(new_path), row['id']),
                )
            results.append({'old': str(old_path), 'new': str(new_path)})

        conn.commit()
    finally:
        conn.close()

    return jsonify({'ok': True, 'new_paths': [r['new'] for r in results]})


@app.post('/api/videos/add-auto')
def api_add_auto():
    volume_path_str = (request.form.get('volume_path') or '').strip()
    volume_path = Path(volume_path_str)
    if volume_path not in [Path(p) for p in MEDIA_LIBRARIES]:
        return jsonify({'error': 'invalid volume_path'}), 400

    files = request.files.getlist('files[]')
    if not files:
        return jsonify({'error': 'no files'}), 400

    job_id = str(uuid.uuid4())
    tmp_dir = volume_path / '.upload_tmp' / job_id
    tmp_dir.mkdir(parents=True, exist_ok=True)

    saved_paths = []
    for f in files:
        safe_name = secure_filename(f.filename) or f'file_{len(saved_paths)}.bin'
        dest = tmp_dir / safe_name
        f.save(str(dest))
        saved_paths.append(dest)

    with _jobs_lock:
        job = _Job(job_id=job_id)
        _jobs[job_id] = job

    t = threading.Thread(
        target=_run_auto_pipeline,
        args=(job, saved_paths, volume_path),
        daemon=True,
    )
    t.start()
    return jsonify({'job_id': job_id})


@app.get('/api/jobs/<string:job_id>/stream')
def api_job_stream(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({'error': 'not found'}), 404

    def generate():
        while True:
            try:
                event = job.queue.get(timeout=30)
            except queue.Empty:
                yield 'data: {"step":"ping","status":"running","message":""}\n\n'
                continue
            yield f'data: {json.dumps(event, ensure_ascii=False)}\n\n'
            if event.get('step') in ('done', 'error'):
                break

    resp = Response(
        generate(),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )
    resp.headers['Content-Type'] = 'text/event-stream'
    return resp


@app.get('/api/videos')
def api_videos():
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    sort = request.args.get('sort', 'date')
    year = request.args.get('year') or None
    score_min = request.args.get('score_min', type=float)
    score_max = request.args.get('score_max', type=float)
    has_cover = request.args.get('has_cover') == '1'
    has_translation = request.args.get('has_translation') == '1'
    scrape_status = request.args.get('scrape_status') or None
    tags = _selected_tags()
    return jsonify(list_videos(
        page=page, limit=limit, sort=sort,
        year=year, score_min=score_min, score_max=score_max,
        has_cover=has_cover, has_translation=has_translation,
        scrape_status=scrape_status, tags=tags,
    ))


@app.get('/api/videos/<string:code>')
def api_video_detail(code: str):
    data = get_video_by_code(code)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


@app.patch('/api/videos/<string:code>')
def api_video_patch(code: str):
    body = request.get_json(silent=True) or {}
    actor_names = body.pop('actors', None)
    tag_names = body.pop('tags', None)
    new_code = body.pop('code', None)
    cluster_id = body.pop('series_cluster_id', None)
    changed = False
    # If user picked a cluster, assign via cluster logic (merge old series in)
    if cluster_id:
        changed = assign_series_cluster(code, int(cluster_id))
        body.pop('series', None)  # don't double-write series
    if body:
        changed = patch_video(code, body) or changed
    if actor_names is not None or tag_names is not None:
        changed = patch_video_relations(code, actor_names, tag_names) or changed
    if new_code and new_code != code:
        if not rename_video_code(code, new_code):
            return jsonify({'error': 'code already exists or not found'}), 400
        changed = True
        code = new_code
    if not changed:
        return jsonify({'error': 'no valid fields or not found'}), 400
    return jsonify({'new_code': code, **get_video_by_code(code)})


@app.patch('/api/videos/<string:code>/file-path')
def api_video_file_path_patch(code: str):
    body = request.get_json(silent=True) or {}
    new_path = body.get('original_file_path') or body.get('origin_file_path')
    result = patch_video_file_path(code, new_path, body.get('part'))
    if not result['ok']:
        status = 409 if 'UNIQUE' in result.get('error', '') else 400
        if result.get('error') == 'not found':
            status = 404
        return jsonify({'error': result.get('error', 'failed')}), status
    return jsonify(result)


@app.post('/api/videos/<string:code>/replace')
def api_video_replace(code: str):
    original_part = request.args.get('part') or None
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'no file'}), 400
    dest_name = (request.form.get('dest_name') or '').strip()
    if not dest_name:
        return jsonify({'error': 'missing dest_name'}), 400
    try:
        expected_size = int(request.form.get('file_size') or 0)
    except ValueError:
        return jsonify({'error': 'invalid file_size'}), 400
    if not expected_size:
        return jsonify({'error': 'file_size must be positive'}), 400

    subtitle_zh = request.form.get('subtitle_zh') == '1'
    uncensored_leaked = request.form.get('uncensored_leaked') == '1'
    uncensored_native = request.form.get('uncensored_native') == '1'
    dest_part = (request.form.get('part') or '').strip() or None

    original_path = get_video_file_path(code, original_part)
    if not original_path:
        return jsonify({'error': 'video not found'}), 404

    final_name = _compute_final_filename(dest_name, subtitle_zh, uncensored_leaked, uncensored_native, dest_part)
    dest_path = Path(original_path).parent / final_name
    temp_path = Path(str(dest_path) + '.temp')

    try:
        f.save(str(temp_path))
        actual_size = os.path.getsize(str(temp_path))
        if actual_size != expected_size:
            temp_path.unlink(missing_ok=True)
            return jsonify({'error': f'size mismatch: expected {expected_size}, got {actual_size}'}), 400
        # DB update first — filesystem still intact if this fails
        result = replace_video_file(code, original_part, str(dest_path), subtitle_zh, uncensored_leaked, uncensored_native)
        if not result['ok']:
            temp_path.unlink(missing_ok=True)
            return jsonify({'error': result.get('error', 'db update failed')}), 500

        # Filesystem swap — DB already points to new path
        if Path(original_path) != dest_path:
            Path(original_path).unlink(missing_ok=True)
        temp_path.rename(dest_path)
    except Exception as e:
        temp_path.unlink(missing_ok=True)
        return jsonify({'error': str(e)}), 500

    return jsonify({'ok': True, **get_video_by_code(code)})


_MIME_MAP = {
    '.mp4': 'video/mp4',
    '.m4v': 'video/mp4',
    '.mov': 'video/quicktime',
    '.avi': 'video/x-msvideo',
    '.wmv': 'video/x-ms-wmv',
    '.mkv': 'video/x-matroska',
    '.webm': 'video/webm',
    '.flv': 'video/x-flv',
}

def _do_stream(code: str, part: str = None):
    path = get_video_file_path(code, part)
    if not path or not os.path.exists(path):
        return jsonify({'error': 'file not found'}), 404
    ext = Path(path).suffix.lower()
    mime = _MIME_MAP.get(ext, 'video/mp4')
    resp = send_file(path, mimetype=mime, conditional=True)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Range'
    resp.headers['Access-Control-Expose-Headers'] = 'Content-Range, Accept-Ranges, Content-Length'
    resp.headers['Accept-Ranges'] = 'bytes'
    return resp


@app.get('/api/videos/<string:code>/stream')
def api_video_stream(code: str):
    return _do_stream(code, request.args.get('part') or None)


# 带扩展名的别名路由，供 VRPlayer 等依赖 URL 后缀判断格式的播放器使用
@app.get('/api/videos/<string:code>/stream.<string:ext>')
def api_video_stream_ext(code: str, ext: str):
    return _do_stream(code, request.args.get('part') or None)


@app.route('/api/videos/<string:code>/stream', methods=['OPTIONS'])
@app.route('/api/videos/<string:code>/stream.<string:ext>', methods=['OPTIONS'])
def api_video_stream_options(code: str, **kwargs):
    resp = app.make_default_options_response()
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Range'
    return resp


@app.delete('/api/videos/<string:code>')
def api_video_delete(code: str):
    result = delete_video(code)
    if not result['ok']:
        return jsonify({'error': result.get('error', 'failed')}), 404
    return jsonify(result)


_COVERS_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'covers')

@app.post('/api/videos/<string:code>/cover')
def api_video_upload_cover(code: str):
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'no file'}), 400
    ext = Path(secure_filename(f.filename)).suffix.lower()
    if ext not in {'.jpg', '.jpeg', '.png', '.webp'}:
        return jsonify({'error': 'unsupported file type'}), 400
    os.makedirs(_COVERS_DIR, exist_ok=True)
    filename = f'{code}{ext}'
    f.save(os.path.join(_COVERS_DIR, filename))
    rel_path = f'covers/{filename}'
    if not patch_video_cover(code, rel_path):
        return jsonify({'error': 'video not found'}), 404
    return jsonify({'cover_path': rel_path})


@app.get('/api/search')
def api_search():
    q = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    return jsonify(search_videos(q, page=page, limit=limit))


@app.get('/api/actors')
def api_actors_search():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    return jsonify(search_actors(q))


@app.get('/api/series')
def api_series_search():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    return jsonify(search_series(q))


@app.get('/api/stats')
def api_stats():
    return jsonify(get_stats())


@app.get('/api/recommendations')
def api_recommendations():
    return jsonify(get_recommendations(count=8, tags=_selected_tags()))


@app.get('/api/tags/all')
def api_tags_all():
    return jsonify(list_all_tags_with_count())


@app.get('/api/actors/all')
def api_actors_all():
    return jsonify(list_all_actors_with_count())


@app.get('/api/series/all')
def api_series_all():
    return jsonify(list_all_series_with_count())


@app.get('/api/series/videos')
def api_series_videos():
    cid = request.args.get('cid', type=int)
    s = request.args.get('s', '').strip() or None
    if not cid and not s:
        return jsonify({'error': 'missing cid or s'}), 400
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    sort = request.args.get('sort', 'date')
    return jsonify(get_videos_by_series(series_name=s, cluster_id=cid, page=page, limit=limit, sort=sort))


@app.get('/api/prefixes/all')
def api_prefixes_all():
    return jsonify(list_all_prefixes_with_count())


@app.get('/api/prefixes/videos')
def api_prefix_videos():
    prefix = request.args.get('prefix', '').strip()
    if not prefix:
        return jsonify({'error': 'missing prefix'}), 400
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    sort = request.args.get('sort', 'date')
    return jsonify(get_videos_by_prefix(prefix, page=page, limit=limit, sort=sort))


@app.get('/api/actors/<int:actor_id>')
def api_actor(actor_id: int):
    data = get_actor_with_videos(actor_id)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


@app.patch('/api/actors/<int:actor_id>')
def api_actor_patch(actor_id: int):
    body = request.get_json(silent=True) or {}
    new_name = body.get('name', '').strip()

    # Check if renaming `name` would collide with an existing actor → merge
    if new_name:
        conflict = find_actor_by_name(new_name, exclude_id=actor_id)
        if conflict:
            merge_actors(source_id=actor_id, target_id=conflict['id'])
            data = get_actor_with_videos(conflict['id'])
            return jsonify({'merged': True, 'actor': data['actor'], 'redirect_id': conflict['id']})

    if not patch_actor(actor_id, body):
        return jsonify({'error': 'no valid fields or not found'}), 400
    data = get_actor_with_videos(actor_id)
    return jsonify({'merged': False, 'actor': data['actor']})


_AVATARS_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'avatars')
_ALLOWED_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}

@app.post('/api/actors/<int:actor_id>/avatar')
def api_actor_upload_avatar(actor_id: int):
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'no file'}), 400
    ext = Path(secure_filename(f.filename)).suffix.lower()
    if ext not in _ALLOWED_EXT:
        return jsonify({'error': 'unsupported file type'}), 400
    os.makedirs(_AVATARS_DIR, exist_ok=True)
    filename = f'actor_{actor_id}{ext}'
    save_path = os.path.join(_AVATARS_DIR, filename)
    f.save(save_path)
    rel_path = f'avatars/{filename}'
    if not patch_actor_avatar(actor_id, rel_path):
        return jsonify({'error': 'actor not found'}), 404
    return jsonify({'avatar_path': rel_path})


@app.get('/api/tags/<int:tag_id>')
def api_tag(tag_id: int):
    data = get_tag_with_videos(tag_id)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), '..', 'templates')

@app.get('/', defaults={'path': ''})
@app.get('/<path:path>')
def index(path):
    return send_from_directory(_TEMPLATES_DIR, 'index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False)

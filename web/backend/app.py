import json
import logging
import os
import queue
import random
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from flask import Flask, Response, jsonify, request, send_file, send_from_directory, url_for
from werkzeug.utils import secure_filename

from config import DB_PATH, MEDIA_LIBRARIES
from core.scanner import extract_video_code
from core.translator.service import shorten_chinese_title_with_candidates, translate_title
from utils.rename_and_move_videos import (
    VideoRecord,
    build_destination,
    build_filename,
    sanitize_component,
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
    rename_video_file,
    replace_multipart_with_single_file,
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
_javdb_context_lock = threading.Lock()
_javdb_session = None

_JAVDB_REFRESH_BASE_SECONDS = 30 * 60
_JAVDB_REFRESH_JITTER_MIN_SECONDS = 5 * 60
_JAVDB_REFRESH_JITTER_MAX_SECONDS = 10 * 60


def _cleanup_old_jobs():
    while True:
        time.sleep(600)
        cutoff = time.time() - 3600
        with _jobs_lock:
            stale = [jid for jid, j in _jobs.items() if j.created_at < cutoff]
            for jid in stale:
                del _jobs[jid]


threading.Thread(target=_cleanup_old_jobs, daemon=True).start()


def _next_javdb_refresh_delay() -> float:
    jitter = random.uniform(
        _JAVDB_REFRESH_JITTER_MIN_SECONDS,
        _JAVDB_REFRESH_JITTER_MAX_SECONDS,
    )
    return _JAVDB_REFRESH_BASE_SECONDS + random.choice((-1, 1)) * jitter


class _JavdbSession:
    def __init__(self):
        self.commands = queue.Queue()
        self.thread = None
        self.context = None
        self.page = None
        self.ready = threading.Event()
        self.stopped = threading.Event()
        self.start_error = None

    def start(self):
        if self.thread is not None and self.thread.is_alive():
            return
        self.thread = threading.Thread(
            target=self._run,
            name='javdb-browser',
            daemon=True,
        )
        self.thread.start()
        if not self.ready.wait(timeout=120):
            raise TimeoutError('启动 JavDB 浏览器超时')
        if self.start_error is not None:
            raise RuntimeError(f'启动 JavDB 浏览器失败: {self.start_error}') from self.start_error

    def _run(self):
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        from core.scraper import load_javdb_cookies, launch_javdb_context, javdb_profile_lock

        try:
            with javdb_profile_lock():
                with sync_playwright() as playwright:
                    self.context = launch_javdb_context(playwright, headless=False)
                    load_javdb_cookies(self.context)
                    self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
                    Stealth().apply_stealth_sync(self.page)
                    self.page.goto('about:blank')
                    self.ready.set()
                    logging.info('JavDB 常驻浏览器已启动，等待手动进入网站')
                    self._serve_commands(Stealth())
        except BaseException as exc:
            if not self.ready.is_set():
                self.start_error = exc
            else:
                logging.exception('JavDB 常驻浏览器线程异常退出')
        finally:
            if self.context is not None:
                try:
                    self.context.close()
                except Exception:
                    pass
                self.context = None
            self.page = None
            self.stopped.set()
            self.ready.set()

    def _live_page(self, stealth):
        if self.page is not None and not self.page.is_closed():
            return self.page
        pages = [page for page in self.context.pages if not page.is_closed()]
        self.page = pages[-1] if pages else self.context.new_page()
        stealth.apply_stealth_sync(self.page)
        return self.page

    def _refresh(self, stealth):
        page = self._live_page(stealth)
        if not page.url.startswith(('http://', 'https://')):
            return
        try:
            page.reload(wait_until='domcontentloaded', timeout=30000)
            logging.info('JavDB 常驻页面已定时刷新: %s', page.url)
        except Exception:
            logging.exception('JavDB 常驻页面定时刷新失败，session 保持运行')

    def _serve_commands(self, stealth):
        from core.scraper import scrape_video_info

        next_refresh = time.monotonic() + _next_javdb_refresh_delay()
        while True:
            timeout = max(0.0, next_refresh - time.monotonic())
            try:
                action, payload, result_queue = self.commands.get(timeout=timeout)
            except queue.Empty:
                action = None

            if action == 'close':
                return
            if action == 'scrape':
                try:
                    result_queue.put((True, scrape_video_info(self._live_page(stealth), payload)))
                except BaseException as exc:
                    result_queue.put((False, exc))

            if time.monotonic() >= next_refresh:
                self._refresh(stealth)
                next_refresh = time.monotonic() + _next_javdb_refresh_delay()

    def scrape(self, base_code: str) -> dict:
        if self.thread is None or not self.thread.is_alive():
            raise RuntimeError('JavDB 浏览器 session 未运行')
        result_queue = queue.Queue(maxsize=1)
        self.commands.put(('scrape', base_code, result_queue))
        while True:
            try:
                ok, value = result_queue.get(timeout=1)
                break
            except queue.Empty:
                if self.stopped.is_set():
                    raise RuntimeError('JavDB 浏览器 session 已意外退出')
        if not ok:
            raise value
        return value

    def close(self):
        if self.thread is not None and self.thread.is_alive():
            self.commands.put(('close', None, None))
            self.thread.join(timeout=60)


def _get_javdb_session() -> '_JavdbSession':
    global _javdb_session
    if _javdb_session is None:
        _javdb_session = _JavdbSession()
        _javdb_session.start()
    return _javdb_session


import atexit
atexit.register(lambda: _javdb_session and _javdb_session.close())


def _scrape_with_playwright(base_code: str) -> dict:
    """Run JavDB scrape through a long-lived browser session."""
    with _javdb_context_lock:
        session = _get_javdb_session()
        return session.scrape(base_code)


@app.post('/api/javdb/session/close')
def api_javdb_session_close():
    global _javdb_session
    with _javdb_context_lock:
        if _javdb_session is not None:
            _javdb_session.close()
            _javdb_session = None
    return jsonify({'ok': True})


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
        from core.scraper import FREEZE

        conn_tmp = sqlite3.connect(DB_PATH)
        try:
            cur = conn_tmp.cursor()
            cur.execute('SELECT id FROM videos WHERE code = ? LIMIT 1', (main_code,))
            row = cur.fetchone()
        finally:
            conn_tmp.close()

        if result_data == FREEZE:
            if row:
                from dal.db_manager import update_video_metadata
                update_video_metadata(row[0], 'FREEZE')
            emit('scrape', 'done', '未找到精确匹配，已冻结待人工')
            emit('actors', 'done', '跳过')
        elif result_data:
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
        _rename_files_for_code(main_code)
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


def _rename_files_for_code(main_code: str) -> None:
    """Rebuild on-disk filenames from current DB metadata for all parts of a code."""
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
            new_path = _build_current_library_destination(rec)
            if old_path != new_path and old_path.exists():
                new_path.parent.mkdir(parents=True, exist_ok=True)
                old_path.rename(new_path)
                cur.execute(
                    'UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP '
                    'WHERE id = ?',
                    (str(new_path), vrow['id']),
                )
        final_conn.commit()
    finally:
        final_conn.close()


def _library_root_for_path(path: Path) -> Optional[Path]:
    for parent in path.parents:
        if parent.name.lower() == 'avideo':
            return parent
    return None


def _build_current_library_destination(record: VideoRecord) -> Path:
    library_root = _library_root_for_path(record.original_file_path)
    if library_root is None:
        return record.original_file_path.parent / build_filename(record)
    return build_destination(record, dest_root=library_root)


def _rescrape_one_code(main_code: str) -> dict:
    """Re-scrape one existing code: scrape → update metadata → translate → short title → rename."""
    base_code = re.sub(r'(?i)-(?:uc|u|c)$', '', main_code)
    result_data = _scrape_with_playwright(base_code)

    conn_tmp = sqlite3.connect(DB_PATH)
    try:
        cur = conn_tmp.cursor()
        cur.execute('SELECT id FROM videos WHERE code = ? LIMIT 1', (main_code,))
        row = cur.fetchone()
    finally:
        conn_tmp.close()
    if not row:
        return {'ok': False, 'message': '未找到记录'}

    from dal.db_manager import update_video_metadata
    from core.scraper import FREEZE
    if result_data == FREEZE:
        update_video_metadata(row[0], 'FREEZE')
        return {'ok': False, 'message': '搜索结果无精确匹配，已冻结待人工'}
    update_video_metadata(row[0], 'SUCCESS' if result_data else 'FAILED', result_data)
    if not result_data:
        return {'ok': False, 'message': '刮削未找到结果'}

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
    if title_zh:
        try:
            title_zh_short, _ = shorten_chinese_title_with_candidates(
                title_zh, actor_names=actor_names
            )
            if title_zh_short:
                patch_video(main_code, {'title_zh_short': title_zh_short})
        except Exception:
            logging.exception(f'[{main_code}] 计算短标题失败')

    _rename_files_for_code(main_code)
    return {'ok': True, 'message': title_zh or '刮削完成'}


def _run_rescrape_pipeline(job: '_Job', codes: list) -> None:
    def emit(step: str, status: str, message: str, **extra):
        job.queue.put({'step': step, 'status': status, 'message': message, **extra})

    success = 0
    failed = 0
    try:
        for code in codes:
            emit(code, 'running', '刮削中…')
            try:
                res = _rescrape_one_code(code)
            except Exception as exc:
                logging.exception(f'[{code}] 重新刮削失败')
                res = {'ok': False, 'message': str(exc)}
            if res['ok']:
                success += 1
                emit(code, 'done', res['message'])
            else:
                failed += 1
                emit(code, 'error', res['message'])
        emit('done', 'done', f'完成：{success} 成功 / {failed} 失败',
             total=len(codes), success=success, failed=failed)
    finally:
        job.status = 'done'


def _run_single_rescrape_job(job: '_Job', code: str) -> None:
    try:
        job.queue.put({'step': 'scrape', 'status': 'running', 'message': '刮削中…'})
        try:
            res = _rescrape_one_code(code)
        except Exception as exc:
            logging.exception(f'[{code}] 重新刮削失败')
            res = {'ok': False, 'message': str(exc)}

        if res['ok']:
            job.queue.put({'step': 'scrape', 'status': 'done', 'message': res['message']})
            job.queue.put({'step': 'done', 'status': 'done', 'message': '重新刮削完成', 'code': code})
        else:
            job.queue.put({'step': 'error', 'status': 'error', 'message': res['message'], 'code': code})
    finally:
        job.status = 'done'


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
    # The tag suffix belongs on the code (first '.'-separated component), e.g.
    # ABW-123-C.女优.[标题].mp4 — NOT appended to the end of the whole filename.
    components = stem.split('.')
    components[0] = re.sub(r'(?i)-(?:uc|u|c)$', '', components[0]) + suffix
    stem = '.'.join(components)
    part_seg = f'.part{part}' if part else ''
    return f'{stem}{part_seg}{ext}'


def _disk_usage_bytes(path: Path):
    """Return disk usage bytes, preferring df because statvfs is wrong for some macOS external volumes."""
    import shutil as _shutil
    import subprocess

    try:
        output = subprocess.check_output(
            ['df', '-Pk', str(path)],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        lines = [line for line in output.splitlines() if line.strip()]
        if len(lines) >= 2:
            fields = lines[-1].split()
            if len(fields) >= 4:
                total = int(fields[1]) * 1024
                used = int(fields[2]) * 1024
                free = int(fields[3]) * 1024
                return total, used, free
    except Exception:
        pass

    usage = _shutil.disk_usage(str(path))
    return usage.total, usage.used, usage.free


@app.get('/api/config/media-libraries')
def api_media_libraries():
    result = []
    for p in MEDIA_LIBRARIES:
        parts = p.parts
        name = parts[2] if len(parts) > 2 and parts[1] == 'Volumes' else p.name
        entry = {
            'path': str(p),
            'name': name,
            'available': False,
            'free_gb': None,
            'used_gb': None,
            'total_gb': None,
            'used_percent': None,
            'free_percent': None,
        }
        try:
            total, used, free = _disk_usage_bytes(p)
            used_percent = (used / total * 100) if total else 0
            entry['available'] = True
            entry['free_gb'] = round(free / (1024 ** 3), 1)
            entry['used_gb'] = round(used / (1024 ** 3), 1)
            entry['total_gb'] = round(total / (1024 ** 3), 1)
            entry['used_percent'] = round(used_percent, 1)
            entry['free_percent'] = round(100 - used_percent, 1)
        except Exception:
            pass
        result.append(entry)
    return jsonify(result)


@app.post('/api/videos/batch-check')
def api_batch_check():
    data = request.get_json(silent=True) or {}
    filenames = data.get('filenames', [])
    if not isinstance(filenames, list):
        return jsonify({'error': 'filenames must be a list'}), 400

    results = []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        for filename in filenames:
            if not isinstance(filename, str):
                results.append({
                    'filename': str(filename), 'code': None, 'part': None,
                    'exists': None, 'original_path': None, 'db_parts': [],
                    'error': 'filename must be a string',
                })
                continue
            stem = Path(filename).stem
            code, part = extract_video_code(stem)
            if not code:
                results.append({
                    'filename': filename, 'code': None, 'part': part,
                    'exists': None, 'original_path': None, 'db_parts': [],
                    'error': '无法解析番号',
                })
                continue
            # Suffix-agnostic match: ABW-123, ABW-123-C, ABW-123-U, ABW-123-UC are the
            # same video. Compare on the base code (suffix stripped).
            base = re.sub(r'(?i)-(?:uc|u|c)$', '', code)
            candidates = [base, f'{base}-C', f'{base}-U', f'{base}-UC']
            placeholders = ','.join('?' * len(candidates))
            rows = conn.execute(
                f'SELECT code, original_file_path, part, scrape_status, deleted FROM videos '
                f'WHERE UPPER(code) IN ({placeholders}) '
                f'ORDER BY deleted ASC, id ASC',
                [c.upper() for c in candidates],
            ).fetchall()
            active_rows = [r for r in rows if not r['deleted']]
            if active_rows:
                row = active_rows[0]
                results.append({
                    'filename': filename, 'code': code, 'part': part,
                    'db_code': row['code'],
                    'record_exists': True,
                    'exists': True,
                    'status': 'ACTIVE',
                    'scrape_status': row['scrape_status'],
                    'original_path': row['original_file_path'],
                    'db_parts': [r['part'] for r in active_rows],
                })
            elif rows:
                row = rows[0]
                results.append({
                    'filename': filename, 'code': code, 'part': part,
                    'db_code': row['code'],
                    'record_exists': True,
                    'exists': False,
                    'status': 'DELETED',
                    'scrape_status': row['scrape_status'],
                    'original_path': row['original_file_path'],
                    'db_parts': [r['part'] for r in rows],
                })
            else:
                results.append({
                    'filename': filename, 'code': code, 'part': part,
                    'db_code': None,
                    'record_exists': False,
                    'exists': False,
                    'status': 'NOT_FOUND',
                    'scrape_status': None,
                    'original_path': None, 'db_parts': [],
                })
    finally:
        conn.close()

    return jsonify(results)


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


# ── Streaming part upload (large multi-part batches) ──────────────
# Clients upload each file as a separate raw-body request straight to the
# destination volume, then call add-auto / add-manual with the upload_id.
# This avoids the 50GB single-request cap and Werkzeug's /tmp spooling that
# multipart form uploads incur.
_UPLOAD_ID_RE = re.compile(r'^[0-9a-fA-F-]{36}$')


def _resolve_upload_dir(volume_path: Path, upload_id: str) -> Optional[Path]:
    """Tmp dir for a pre-uploaded part batch, or None if volume/id is invalid."""
    if not _UPLOAD_ID_RE.match(upload_id or ''):
        return None
    if volume_path not in [Path(p) for p in MEDIA_LIBRARIES]:
        return None
    return volume_path / '.upload_tmp' / upload_id


def _list_uploaded_files(tmp_dir: Optional[Path]) -> list:
    """Real uploaded files in an upload tmp dir, sorted by name.

    Excludes macOS metadata sidecars (._* AppleDouble, .DS_Store) — exFAT
    volumes create a 4KB ._<name> file next to every write, and those must
    never be picked up as video parts.
    """
    if not tmp_dir or not tmp_dir.is_dir():
        return []
    return sorted(
        [p for p in tmp_dir.iterdir() if p.is_file() and not p.name.startswith('.')],
        key=lambda p: p.name,
    )


@app.post('/api/videos/upload-part')
def api_upload_part():
    from urllib.parse import unquote
    volume_path = Path(unquote(request.headers.get('X-Volume-Path') or '').strip())
    upload_id = (request.headers.get('X-Upload-Id') or '').strip()
    tmp_dir = _resolve_upload_dir(volume_path, upload_id)
    if tmp_dir is None:
        return jsonify({'error': 'invalid volume_path or upload_id'}), 400

    safe_name = secure_filename(unquote(request.headers.get('X-Dest-Name') or '').strip())
    if not safe_name:
        return jsonify({'error': 'invalid filename'}), 400
    try:
        expected_size = int(request.headers.get('X-File-Size') or 0)
    except (ValueError, TypeError):
        return jsonify({'error': 'invalid X-File-Size'}), 400

    tmp_dir.mkdir(parents=True, exist_ok=True)
    dest = tmp_dir / safe_name
    written = 0
    with open(str(dest), 'wb') as dst:
        while chunk := request.stream.read(4 * 1024 * 1024):
            dst.write(chunk)
            written += len(chunk)
    if expected_size and written != expected_size:
        dest.unlink(missing_ok=True)
        return jsonify({'error': f'size mismatch: expected {expected_size}, got {written}'}), 400
    return jsonify({'ok': True, 'name': safe_name})


@app.post('/api/videos/add-manual')
def api_add_manual():
    payload = request.get_json(silent=True) if request.is_json else None
    use_upload = bool(payload and payload.get('upload_id'))
    src = payload if use_upload else request.form

    volume_path = Path((src.get('volume_path') or '').strip())
    if volume_path not in [Path(p) for p in MEDIA_LIBRARIES]:
        return jsonify({'error': 'invalid volume_path'}), 400

    code_type = (src.get('code_type') or '').strip().upper()
    if code_type not in ('WEST', 'CN', 'DUMMY'):
        return jsonify({'error': 'invalid code_type'}), 400

    try:
        code_number = int(src.get('code_number') or 0)
    except (ValueError, TypeError):
        return jsonify({'error': 'invalid code_number'}), 400
    if code_number < 1:
        return jsonify({'error': 'code_number must be >= 1'}), 400

    is_multipart = src.get('is_multipart') in ('1', 1, True)

    # Gather the part files: pre-streamed (upload_id) or legacy multipart form.
    if use_upload:
        tmp_dir = _resolve_upload_dir(volume_path, payload['upload_id'])
        part_paths = _list_uploaded_files(tmp_dir)
    else:
        files = request.files.getlist('files[]')
        tmp_dir = volume_path / '.upload_tmp' / str(uuid.uuid4())
        part_paths = []
        if files:
            tmp_dir.mkdir(parents=True, exist_ok=True)
            for i, f in enumerate(sorted(files, key=lambda f: secure_filename(f.filename) or '')):
                p = tmp_dir / (secure_filename(f.filename) or f'file_{i}.bin')
                f.save(str(p))
                part_paths.append(p)
    if not part_paths:
        return jsonify({'error': 'no files'}), 400

    code = f'{code_type}-{code_number:03d}'
    scanned = []
    try:
        for i, tmp_path in enumerate(part_paths):
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

        # Manual uploads carry user-provided metadata — no scraping needed.
        batch_insert_scanned_videos(scanned, scrape_status='SUCCESS')
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
            new_path = _build_current_library_destination(record)
            if old_path != new_path:
                cursor.execute(
                    "SELECT id FROM videos WHERE original_file_path = ? AND id != ? LIMIT 1",
                    (str(new_path), row['id']),
                )
                if cursor.fetchone():
                    return jsonify({'error': f'destination path already in DB: {new_path}'}), 409
            if old_path != new_path and new_path.exists():
                return jsonify({'error': f'destination exists: {new_path}'}), 409
            if old_path != new_path and old_path.exists():
                new_path.parent.mkdir(parents=True, exist_ok=True)
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
    payload = request.get_json(silent=True) if request.is_json else None
    if payload and payload.get('upload_id'):
        volume_path = Path((payload.get('volume_path') or '').strip())
        tmp_dir = _resolve_upload_dir(volume_path, payload['upload_id'])
        if tmp_dir is None:
            return jsonify({'error': 'invalid volume_path'}), 400
        saved_paths = _list_uploaded_files(tmp_dir)
        if not saved_paths:
            return jsonify({'error': 'no files'}), 400
        job_id = payload['upload_id']
    else:
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


@app.post('/api/videos/rescrape')
def api_rescrape():
    data = request.get_json(silent=True) or {}
    status = (data.get('scrape_status') or '').strip().upper()
    if status not in ('FAILED', 'PENDING'):
        return jsonify({'error': 'scrape_status must be FAILED or PENDING'}), 400

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            'SELECT DISTINCT code FROM videos WHERE scrape_status = ? AND deleted = 0 ORDER BY code',
            (status,),
        ).fetchall()
    finally:
        conn.close()
    codes = [r[0] for r in rows]
    if not codes:
        return jsonify({'error': '没有符合条件的影片'}), 400

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        job = _Job(job_id=job_id)
        _jobs[job_id] = job

    t = threading.Thread(target=_run_rescrape_pipeline, args=(job, codes), daemon=True)
    t.start()
    return jsonify({'job_id': job_id, 'codes': codes})


@app.post('/api/videos/<string:code>/rescrape')
def api_video_rescrape(code: str):
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            'SELECT code FROM videos WHERE code = ? AND deleted = 0 LIMIT 1',
            (code,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return jsonify({'error': 'not found'}), 404
    main_code = row[0]

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        job = _Job(job_id=job_id)
        _jobs[job_id] = job

    t = threading.Thread(target=_run_single_rescrape_job, args=(job, main_code), daemon=True)
    t.start()
    return jsonify({'job_id': job_id, 'code': main_code})


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


@app.get('/api/videos/files')
def api_video_files():
    extension = (request.args.get('extension') or 'mkv').strip().lower().lstrip('.')
    if not extension or not re.fullmatch(r'[a-z0-9]{1,10}', extension):
        return jsonify({'error': 'invalid extension'}), 400

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT v.id, v.code, v.part, v.original_file_path, v.file_size,
                   EXISTS (
                       SELECT 1 FROM video_tag_link vtl
                       JOIN tags t ON t.id = vtl.tag_id
                       WHERE vtl.video_id = v.id AND t.name = '中文字幕'
                   ) AS subtitle_zh,
                   EXISTS (
                       SELECT 1 FROM video_tag_link vtl
                       JOIN tags t ON t.id = vtl.tag_id
                       WHERE vtl.video_id = v.id AND t.name IN ('无码流出', '無碼流出', '无码破解', '無碼破解')
                   ) AS uncensored_leaked,
                   EXISTS (
                       SELECT 1 FROM video_tag_link vtl
                       JOIN tags t ON t.id = vtl.tag_id
                       WHERE vtl.video_id = v.id AND t.name = '原装无码'
                   ) AS uncensored_native
            FROM videos v
            WHERE v.deleted = 0
              AND LOWER(v.original_file_path) LIKE ?
            ORDER BY v.code, v.part, v.id
            """,
            (f'%.{extension}',),
        ).fetchall()
    finally:
        conn.close()

    files = []
    missing = 0
    for row in rows:
        path = Path(row['original_file_path'])
        if not path.is_file():
            missing += 1
            continue
        query = {'part': row['part']} if row['part'] else {}
        files.append({
            'id': row['id'],
            'code': row['code'],
            'part': row['part'],
            'filename': path.name,
            'file_size': row['file_size'] or path.stat().st_size,
            'subtitle_zh': bool(row['subtitle_zh']),
            'uncensored_leaked': bool(row['uncensored_leaked']),
            'uncensored_native': bool(row['uncensored_native']),
            'download_url': url_for(
                'api_video_stream_ext', code=row['code'], ext=extension,
                _external=True, **query,
            ),
            'replace_url': url_for(
                'api_video_replace', code=row['code'],
                _external=True, **query,
            ),
        })

    return jsonify({
        'extension': extension,
        'total': len(files),
        'missing': missing,
        'files': files,
    })


@app.get('/api/videos/<string:code>')
def api_video_detail(code: str):
    data = get_video_by_code(code)
    if not data:
        return jsonify({
            'error': 'not found',
            'record_exists': False,
            'exists': False,
            'status': 'NOT_FOUND',
            'scrape_status': None,
        }), 404
    data['record_exists'] = True
    data['exists'] = not bool(data.get('deleted'))
    data['status'] = 'ACTIVE' if data['exists'] else 'DELETED'
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
    content_type = request.content_type or ''

    # Streaming path: file as raw body, metadata in X-* headers.
    # Writes directly to the destination volume while bytes are arriving, eliminating
    # the double-write penalty (Werkzeug /tmp → dest) that multipart incurs.
    if 'multipart' not in content_type:
        # X-Dest-Name is percent-encoded by the client (HTTP headers can't carry
        # non-Latin-1 chars; filenames are often Chinese).
        from urllib.parse import unquote
        dest_name = unquote(request.headers.get('X-Dest-Name') or '').strip()
        try:
            expected_size = int(request.headers.get('X-File-Size') or 0)
        except (ValueError, TypeError):
            return jsonify({'error': 'invalid X-File-Size'}), 400
        subtitle_zh = request.headers.get('X-Subtitle-Zh') == '1'
        uncensored_leaked = request.headers.get('X-Uncensored-Leaked') == '1'
        uncensored_native = request.headers.get('X-Uncensored-Native') == '1'
        dest_part = (request.headers.get('X-Part') or '').strip() or None
        replace_whole = request.headers.get('X-Replace-Whole') == '1'
        body_stream = request.stream
    else:
        # Legacy multipart path (used by existing tests)
        f = request.files.get('file')
        if not f:
            return jsonify({'error': 'no file'}), 400
        dest_name = (request.form.get('dest_name') or '').strip()
        try:
            expected_size = int(request.form.get('file_size') or 0)
        except ValueError:
            return jsonify({'error': 'invalid file_size'}), 400
        subtitle_zh = request.form.get('subtitle_zh') == '1'
        uncensored_leaked = request.form.get('uncensored_leaked') == '1'
        uncensored_native = request.form.get('uncensored_native') == '1'
        dest_part = (request.form.get('part') or '').strip() or None
        replace_whole = request.form.get('replace_whole') == '1'
        body_stream = f.stream

    if not dest_name:
        return jsonify({'error': 'missing dest_name'}), 400
    if not expected_size:
        return jsonify({'error': 'file_size must be positive'}), 400
    if replace_whole:
        dest_part = None
        p = Path(dest_name)
        dest_name = re.sub(r'(?i)\.part\d+$', '', p.stem) + p.suffix

    original_path = get_video_file_path(code, original_part)
    if not original_path:
        return jsonify({'error': 'video not found'}), 404

    final_name = _compute_final_filename(dest_name, subtitle_zh, uncensored_leaked, uncensored_native, dest_part)
    dest_path = Path(original_path).parent / final_name
    temp_path = Path(str(dest_path) + '.temp')

    try:
        written = 0
        with open(str(temp_path), 'wb') as dst:
            while chunk := body_stream.read(4 * 1024 * 1024):
                dst.write(chunk)
                written += len(chunk)
        if written != expected_size:
            temp_path.unlink(missing_ok=True)
            return jsonify({'error': f'size mismatch: expected {expected_size}, got {written}'}), 400
        # Stat the just-written file so the DB tracks the replacement's actual
        # size/mtime/birthtime (rename preserves the inode, so these stay valid).
        st = temp_path.stat()
        file_meta = {
            'file_size': st.st_size,
            'file_mtime': datetime.fromtimestamp(st.st_mtime).isoformat(),
            'file_birthtime': datetime.fromtimestamp(
                getattr(st, 'st_birthtime', st.st_mtime)
            ).isoformat(),
        }
        # DB update first — filesystem still intact if this fails
        if replace_whole:
            result = replace_multipart_with_single_file(
                code, str(dest_path), subtitle_zh, uncensored_leaked,
                uncensored_native, file_meta=file_meta, keep_part=original_part,
            )
        else:
            result = replace_video_file(code, original_part, str(dest_path), subtitle_zh, uncensored_leaked, uncensored_native, file_meta=file_meta)
        if not result['ok']:
            temp_path.unlink(missing_ok=True)
            return jsonify({'error': result.get('error', 'db update failed')}), 500
        # Filesystem swap — DB already points to new path
        if replace_whole:
            for old_path in result.get('old_paths', []):
                old = Path(old_path)
                if old != dest_path:
                    old.unlink(missing_ok=True)
        elif Path(original_path) != dest_path:
            Path(original_path).unlink(missing_ok=True)
        temp_path.rename(dest_path)
    except Exception as e:
        temp_path.unlink(missing_ok=True)
        return jsonify({'error': str(e)}), 500

    # replace_video_file may re-suffix the code (e.g. -C → -UC), so fetch the
    # detail by the new code, not the original request code.
    final_code = result.get('new_code') or code
    return jsonify({'ok': True, **(get_video_by_code(final_code) or {})})


@app.post('/api/videos/<string:code>/rename-file')
def api_video_rename_file(code: str):
    body = request.get_json(silent=True) or {}
    part = (body.get('part') or '').strip() or None
    new_code = sanitize_component(body.get('code') or '')
    if not new_code:
        return jsonify({'error': 'invalid code'}), 400
    actors = [a for a in (sanitize_component(x) for x in (body.get('actors') or [])) if a]
    title = sanitize_component(body.get('title') or '')

    orig = get_video_file_path(code, part)
    if not orig:
        return jsonify({'error': 'not found'}), 404
    old_path = Path(orig)
    ext = old_path.suffix

    components = [new_code, *actors]
    if title:
        components.append(f'[{title}]')
    if part:
        components.append(part)
    new_name = '.'.join(c for c in components if c) + ext
    new_path = old_path.parent / new_name

    if new_path == old_path:
        return jsonify({'ok': True, **get_video_by_code(code)})
    if not old_path.exists():
        return jsonify({'error': 'source file missing'}), 400
    if new_path.exists():
        return jsonify({'error': 'target exists'}), 409

    try:
        old_path.rename(new_path)
    except OSError as e:
        return jsonify({'error': str(e)}), 500

    result = rename_video_file(code, part, str(new_path), new_code)
    if not result['ok']:
        try:
            new_path.rename(old_path)
        except OSError:
            pass
        return jsonify({'error': result.get('error', 'db update failed')}), 500

    return jsonify({'ok': True, **get_video_by_code(new_code)})


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


def _autostart_javdb_session() -> None:
    try:
        with _javdb_context_lock:
            _get_javdb_session()
    except Exception:
        logging.exception('服务器已启动，但 JavDB 常驻浏览器启动失败')


if os.getenv('AVIDEO_JAVDB_AUTOSTART') == '1':
    _autostart_javdb_session()


if __name__ == '__main__':
    if os.getenv('AVIDEO_JAVDB_AUTOSTART') != '1':
        _autostart_javdb_session()
    app.run(host='0.0.0.0', port=8000, debug=False)

import json
import logging
import os
import re
import sqlite3
import tempfile
import time
import uuid
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Optional
from flask import Flask, Response, jsonify, request, send_file, send_from_directory, url_for
from werkzeug.utils import secure_filename

from config import DB_PATH, MEDIA_LIBRARIES
from core import subtitle_cache
from core.task_policy import is_special_code
from core.subtitle_policy import (
    NO_SUBTITLE, SUBTITLE_CLASSIFICATIONS, VERSION_SUFFIX,
    default_subtitle, stored_subtitle, subtitle_code, validate_subtitle,
)
from core.scanner import VIDEO_EXTENSIONS, extract_video_code
from core.worker_client import ensure_worker
from core.browser_queue import RuntimeStore
from core.scrape_queue import StoredJob
from core.workflows import prepare_upload, _build_current_library_destination
from utils.rename_and_move_videos import (
    VideoRecord,
    build_destination,
    build_filename,
    sanitize_component,
)

from dal import playback
from dal.db_manager import (
    add_existing_tags_to_video,
    validate_generic_tags,
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
    delete_video,
    get_video_file_path,
    patch_actor,
    patch_actor_avatar,
    patch_video,
    patch_video_cover,
    patch_video_file_path,
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

_scrape_store = None


def scrape_store():
    global _scrape_store
    if _scrape_store is None or _scrape_store.path != str(DB_PATH):
        _scrape_store = RuntimeStore(DB_PATH)
    return _scrape_store


@app.post('/api/javdb/session/close')
def api_javdb_session_close():
    from core.service_manager import control_service
    return jsonify(control_service('browser', 'stop'))


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
    if part:
        stem = re.sub(r'(?i)\.part\d+$', '', stem)
    part_number = re.sub(r'(?i)^part', '', part) if part else ''
    part_seg = f'.part{part_number}' if part_number else ''
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

    def media_flags(rows, parsed_code):
        tags = []
        video_ids = [row['id'] for row in rows]
        if video_ids:
            placeholders = ','.join('?' * len(video_ids))
            tags = [row['name'] for row in conn.execute(
                f'SELECT DISTINCT t.name FROM video_tag_link vtl '
                f'JOIN tags t ON t.id = vtl.tag_id '
                f'WHERE vtl.video_id IN ({placeholders}) ORDER BY t.name',
                video_ids,
            ).fetchall()]

        classification = stored_subtitle(tags) if rows else default_subtitle(parsed_code or '')
        uncensored_names = {
            '无码流出', '無碼流出', '无码破解', '無碼破解',
            'JAV 无码破解', 'LADA 无码破解', 'LTX 无码破解', '原装无码',
        }
        uncensored_tags = [tag for tag in tags if tag in uncensored_names]
        suffix = (parsed_code or '').upper()
        suffix_is_uncensored = suffix.endswith(('-U', '-UC'))
        return {
            'has_subtitle': classification is not None and classification != NO_SUBTITLE,
            'is_vr': 'VR' in tags,
            'subtitle_type': classification,
            'subtitle_classification': classification,
            'is_uncensored': bool(uncensored_tags) or suffix_is_uncensored,
            'uncensored_type': uncensored_tags[0] if uncensored_tags else (
                '无码' if suffix_is_uncensored else None
            ),
        }

    try:
        for filename in filenames:
            if not isinstance(filename, str):
                results.append({
                    'filename': str(filename), 'code': None, 'part': None,
                    'exists': None, 'original_path': None, 'db_parts': [],
                    'has_subtitle': None, 'subtitle_type': None, 'subtitle_classification': None, 'is_vr': None,
                    'is_uncensored': None, 'uncensored_type': None,
                    'error': 'filename must be a string',
                })
                continue
            stem = Path(filename).stem
            code, part = extract_video_code(stem)
            if not code:
                results.append({
                    'filename': filename, 'code': None, 'part': part,
                    'exists': None, 'original_path': None, 'db_parts': [],
                    'has_subtitle': None, 'subtitle_type': None, 'subtitle_classification': None, 'is_vr': None,
                    'is_uncensored': None, 'uncensored_type': None,
                    'error': '无法解析番号',
                })
                continue
            # Suffix-agnostic match: ABW-123, ABW-123-C, ABW-123-U, ABW-123-UC are the
            # same video. Compare on the base code (suffix stripped).
            base = re.sub(r'(?i)-(?:uc|u|c)$', '', code)
            candidates = [base, f'{base}-C', f'{base}-U', f'{base}-UC']
            placeholders = ','.join('?' * len(candidates))
            rows = conn.execute(
                f'SELECT id, code, original_file_path, part, scrape_status, deleted FROM videos '
                f'WHERE UPPER(code) IN ({placeholders}) '
                f'ORDER BY deleted ASC, id ASC',
                [c.upper() for c in candidates],
            ).fetchall()
            active_rows = [r for r in rows if not r['deleted']]
            if active_rows:
                exact = next((r['code'] for r in active_rows if r['code'].upper() == code.upper()), None)
                chosen = exact or active_rows[0]['code']
                active_rows = [r for r in active_rows if r['code'] == chosen]
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
                    **media_flags(active_rows, row['code']),
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
                    **media_flags(rows, row['code']),
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
                    **media_flags([], code),
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
            num = int(VERSION_SUFFIX.sub('', code).split('-', 1)[1])
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


def _parse_upload_tags(source, header_value: Optional[str] = None) -> list[str]:
    raw = header_value
    if raw is None:
        raw = source.get('tags') if source is not None else None
    if raw in (None, ''):
        return []
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            decoded = [item.strip() for item in raw.split(',')]
        values = decoded if isinstance(decoded, list) else []
    else:
        values = []
    return list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))


def _missing_existing_tags(tag_names: list[str]) -> list[str]:
    if not tag_names:
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        placeholders = ','.join('?' * len(tag_names))
        rows = conn.execute(
            f'SELECT name FROM tags WHERE name IN ({placeholders})', tag_names
        ).fetchall()
    finally:
        conn.close()
    existing = {row[0] for row in rows}
    return [name for name in tag_names if name not in existing]


def _upload_classification(source, tags):
    validate_generic_tags(tags)
    if 'subtitle_classification' in source:
        return validate_subtitle(source['subtitle_classification'], vr='VR' in tags)
    return NO_SUBTITLE if 'VR' in tags else None


@app.post('/api/videos/upload-part')
def api_upload_part():
    from urllib.parse import unquote
    volume_path = Path(unquote(request.headers.get('X-Volume-Path') or '').strip())
    upload_id = (request.headers.get('X-Upload-Id') or '').strip()
    tmp_dir = _resolve_upload_dir(volume_path, upload_id)
    if tmp_dir is None:
        return jsonify({'error': 'invalid volume_path or upload_id'}), 400

    raw_name = unquote(request.headers.get('X-Dest-Name') or '').strip()
    safe_name = secure_filename(raw_name)
    if not safe_name:
        return jsonify({'error': 'invalid filename'}), 400

    # secure_filename removes separators such as "\\@". Canonicalize a parseable
    # video name first so an ad prefix cannot become part of the code on re-scan
    # (e.g. 489155.com\\@MIAB-679-UC.mp4 -> COMMIAB-679-UC).
    parsed_code, parsed_part = extract_video_code(Path(raw_name).stem)
    safe_code, _ = extract_video_code(Path(safe_name).stem)
    if parsed_code and safe_code != parsed_code:
        suffix = Path(raw_name).suffix.lower()
        part_suffix = f'.{parsed_part}' if parsed_part else ''
        safe_name = secure_filename(f'{parsed_code}{part_suffix}{suffix}')
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
    additional_tags = _parse_upload_tags(src)
    try:
        classification = _upload_classification(src, additional_tags)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    missing_tags = _missing_existing_tags(additional_tags)
    if missing_tags:
        return jsonify({'error': 'unknown tags', 'tags': missing_tags}), 400

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
            for i, f in enumerate(sorted(files, key=lambda f: f.filename or '')):
                p = tmp_dir / (secure_filename(f.filename) or f'file_{i}.bin')
                if p.exists():
                    return jsonify(error='上传批次有重名文件'), 409
                f.save(str(p))
                part_paths.append(p)
    if not part_paths:
        return jsonify({'error': 'no files'}), 400

    code = f'{code_type}-{code_number:03d}'
    if classification is None:
        classification = default_subtitle(next(
            (parsed for path in part_paths if (parsed := extract_video_code(path.stem)[0])
             and parsed.upper().endswith(('-C', '-UC'))), ''
        ))
    code = subtitle_code(code, classification)
    scanned = []
    if len(part_paths) > 1 and not is_multipart:
        return jsonify(error='多个文件需要指定分段'), 400
    destinations = []
    for i, tmp_path in enumerate(part_paths):
        part = f'part{i + 1}' if is_multipart else None
        record = VideoRecord(0, code, part, tmp_path, None, None, None, [])
        dest_path = build_destination(record, dest_root=volume_path)
        if dest_path.exists() or dest_path.is_symlink():
            return jsonify(error='目标目录有重名文件'), 409
        destinations.append((tmp_path, part, dest_path))
    try:
        for tmp_path, part, dest_path in destinations:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with dest_path.open('xb'):
                pass
            try:
                tmp_path.replace(dest_path)
            except OSError:
                dest_path.unlink()
                raise

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
        sync_derived_video_tags_for_code(code, subtitle_classification=classification)
        if additional_tags and not add_existing_tags_to_video(code, additional_tags):
            raise ValueError('添加标签失败')
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
    crack_tags = {'JAV 无码破解', 'LADA 无码破解', 'LTX 无码破解'}
    if payload is not None and 'uncensored_crack_tag' in payload:
        uncensored_crack_tag = payload.get('uncensored_crack_tag') or ''
    elif 'uncensored_crack_tag' in request.form:
        uncensored_crack_tag = request.form.get('uncensored_crack_tag') or ''
    else:
        uncensored_crack_tag = 'LTX 无码破解'
    if uncensored_crack_tag and uncensored_crack_tag not in crack_tags:
        return jsonify({'error': 'invalid uncensored_crack_tag'}), 400
    source = payload if payload is not None else request.form
    additional_tags = _parse_upload_tags(source)
    missing_tags = _missing_existing_tags(additional_tags)
    if missing_tags:
        return jsonify({'error': 'unknown tags', 'tags': missing_tags}), 400
    try:
        subtitle_classification = _upload_classification(source, additional_tags)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    if payload and payload.get('upload_id'):
        volume_path = Path((payload.get('volume_path') or '').strip())
        tmp_dir = _resolve_upload_dir(volume_path, payload['upload_id'])
        if tmp_dir is None:
            return jsonify({'error': 'invalid volume_path'}), 400
        existing = scrape_store().get(payload['upload_id'])
        if existing:
            if existing['kind'] != 'pipeline_auto':
                return jsonify(error='upload_id 已用于其他任务'), 409
            return jsonify(job_id=existing['id']), 202
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

    task_payload = {
        'file_paths': [str(path) for path in saved_paths], 'volume_path': str(volume_path),
        'uncensored_crack_tag': uncensored_crack_tag, 'subtitle_classification': subtitle_classification,
        'additional_tags': additional_tags,
    }
    with scrape_store().connect(write=True) as conn:
        added = conn.execute("""INSERT OR IGNORE INTO scrape_tasks
            (id,kind,status,payload_json,owner,created_at,queued_at)
            VALUES (?,'pipeline_auto','PREPARING',?,?,?,?)""",
            (job_id, json.dumps(task_payload, ensure_ascii=False), str(os.getpid()), time.time(), time.time())).rowcount
    if not added:
        return jsonify(job_id=job_id), 202
    try:
        task_payload['main_code'] = prepare_upload(
            StoredJob(scrape_store(), job_id), saved_paths, volume_path,
            uncensored_crack_tag, subtitle_classification, additional_tags)
    except (ValueError, OSError) as exc:
        with scrape_store().connect(write=True) as conn:
            conn.execute("UPDATE scrape_tasks SET status='FAILED',error=? WHERE id=?", (str(exc), job_id))
        return jsonify(error=str(exc), job_id=job_id), 400
    with scrape_store().connect(write=True) as conn:
        conn.execute("UPDATE scrape_tasks SET code=?,payload_json=?,status='PENDING' WHERE id=?",
                     (task_payload['main_code'], json.dumps(task_payload, ensure_ascii=False), job_id))
    ensure_worker()
    return jsonify({'job_id': job_id}), 202


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
    codes = [r[0] for r in rows if not is_special_code(r[0])]
    if not codes:
        return jsonify({'error': '没有符合条件的影片'}), 400

    job_id = str(uuid.uuid4())
    scrape_store().submit('pipeline_rescrape', payload={'codes': codes}, task_id=job_id)
    ensure_worker()
    return jsonify({'job_id': job_id, 'codes': codes}), 202


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
    if is_special_code(main_code):
        return jsonify(error='特殊番号不参与刮削'), 400

    job_id = str(uuid.uuid4())
    scrape_store().submit('pipeline_rescrape', main_code, payload={'codes': [main_code]}, task_id=job_id)
    ensure_worker()
    return jsonify({'job_id': job_id, 'code': main_code}), 202


from web.backend.scrape_api import register_scrape_api
from web.backend.admin_api import register_admin_api
register_scrape_api(app)
register_admin_api(app)


@app.get('/api/jobs/<string:job_id>/stream')
def api_job_stream(job_id: str):
    stored = scrape_store().get(job_id)
    if stored:
        try:
            after = int(request.headers.get('Last-Event-ID') or request.args.get('after') or 0)
        except ValueError:
            after = 0

        def durable_events():
            cursor = after
            last_ping = time.monotonic()
            while True:
                for row in scrape_store().events(job_id, cursor):
                    cursor = row['id']
                    yield f"id: {cursor}\ndata: {row['event_json']}\n\n"
                    if json.loads(row['event_json']).get('step') in ('done', 'error'):
                        return
                task = scrape_store().get(job_id)
                if task['status'] in ('DONE', 'FAILED', 'SKIPPED', 'CANCELLED'):
                    event = {'step': 'done' if task['status'] == 'DONE' else 'error',
                             'status': task['status'].lower(), 'message': task['error'] or '任务已完成', 'code': task['code']}
                    yield 'data: ' + json.dumps(event, ensure_ascii=False) + '\n\n'
                    return
                if time.monotonic() - last_ping > 15:
                    yield 'data: {"step":"ping","status":"running","message":""}\n\n'
                    last_ping = time.monotonic()
                time.sleep(0.5)

        return Response(durable_events(), mimetype='text/event-stream',
                        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})
    return jsonify({'error': 'not found'}), 404


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
        scrape_status=scrape_status, tags=tags, exclude_tags=request.args.get('exclude') == '1',
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
                   (SELECT CASE WHEN COUNT(DISTINCT t.name)=1 THEN MIN(t.name) END
                    FROM video_tag_link vtl JOIN tags t ON t.id=vtl.tag_id
                    WHERE vtl.video_id=v.id AND t.name IN (?, ?, ?, ?)) AS subtitle_classification,
                   EXISTS (
                       SELECT 1 FROM video_tag_link vtl
                       JOIN tags t ON t.id = vtl.tag_id
                       WHERE vtl.video_id = v.id AND t.name IN (
                           '无码流出', '無碼流出', '无码破解', '無碼破解',
                           'JAV 无码破解', 'LADA 无码破解', 'LTX 无码破解'
                       )
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
            (*SUBTITLE_CLASSIFICATIONS, f'%.{extension}'),
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
            'subtitle_classification': row['subtitle_classification'],
            'subtitle_zh': row['subtitle_classification'] not in (None, NO_SUBTITLE),
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


@app.get('/api/videos/subtitle-review')
def api_subtitle_review():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS subtitle_classification_reviews ("
            "code TEXT PRIMARY KEY COLLATE NOCASE, classification TEXT NOT NULL, "
            "reviewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        rows = conn.execute(
            """
            SELECT v.code,
                   MIN(v.cover_path) AS cover_path,
                   MIN(COALESCE(v.title_zh, v.title_jp, '')) AS title,
                   COUNT(*) AS file_count,
                   ROUND(SUM(COALESCE(v.file_size, 0)) / 1073741824.0, 2) AS size_gib,
                   (SELECT CASE WHEN COUNT(DISTINCT t.name)=1 THEN MIN(t.name) END
                    FROM videos v2 JOIN video_tag_link l2 ON l2.video_id=v2.id
                    JOIN tags t ON t.id=l2.tag_id
                    WHERE v2.code=v.code AND v2.deleted=0 AND t.name IN (?, ?, ?, ?)) AS classification
            FROM videos v
            WHERE v.deleted = 0
              AND LOWER(v.original_file_path) LIKE '%.mkv'
              AND NOT EXISTS (
                  SELECT 1 FROM subtitle_classification_reviews r
                  WHERE r.code = v.code
              )
            GROUP BY v.code
            ORDER BY v.code
            """, SUBTITLE_CLASSIFICATIONS
        ).fetchall()
        return jsonify([dict(row) for row in rows])
    finally:
        conn.close()


@app.post('/api/videos/<string:code>/subtitle-classification')
def api_subtitle_classification(code: str):
    body = request.get_json(silent=True)
    try:
        classification = validate_subtitle(body.get('classification') if isinstance(body, dict) else None)
        video = get_video_by_code(code)
        if not video or video['deleted']:
            return jsonify(error='video not found'), 404
        new_code, jobs = patch_video(code, {}, runtime=scrape_store(),
                                     subtitle_classification=classification, reviewed=True)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except OSError as exc:
        return jsonify(error=str(exc)), 409
    return jsonify(ok=True, code=new_code, classification=classification, rename_job_ids=jobs)


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


@app.get('/api/videos/<string:code>/download-url')
def api_video_download_url(code: str):
    data = get_video_by_code(code)
    if not data or data.get('deleted'):
        return jsonify({'error': 'not found'}), 404

    base_code = re.sub(r'(?i)-(?:uc|u|c)$', '', data['code'])
    source_files = data.get('parts') or [{
        'part': data.get('part'),
        'original_file_path': data.get('original_file_path'),
        'file_size': data.get('file_size'),
    }]
    files = []
    for source in source_files:
        path = Path(source.get('original_file_path') or '')
        ext = path.suffix.lower().lstrip('.') or 'mp4'
        query = {'download': 1}
        if source.get('part'):
            query['part'] = source['part']
        files.append({
            'part': source.get('part'),
            'filename': path.name,
            'file_size': source.get('file_size'),
            'download_url': url_for(
                'api_video_stream_ext', code=data["code"], ext=ext,
                _external=True, **query,
            ),
        })

    return jsonify({
        'input_code': code,
        'code': data['code'],
        'base_code': base_code,
        'download_url': files[0]['download_url'] if len(files) == 1 else None,
        'files': files,
    })


@app.patch('/api/videos/<string:code>')
def api_video_patch(code: str):
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify(error='无效的影片信息'), 400
    actor_names = body.pop('actors', None)
    tag_names = body.pop('tags', None)
    new_code = body.pop('code', None)
    classification_supplied = 'subtitle_classification' in body
    classification = body.pop('subtitle_classification', None)
    if 'series' in body and body['series'] is not None and not isinstance(body['series'], str):
        return jsonify(error='系列名必须是文本'), 400
    try:
        if classification_supplied:
            validate_subtitle(classification)
        code, jobs = patch_video(code, body, runtime=scrape_store(), actor_names=actor_names,
                                 tag_names=tag_names, new_code=new_code, subtitle_classification=classification)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except OSError as exc:
        return jsonify(error=str(exc)), 409
    except sqlite3.Error:
        app.logger.exception('保存影片信息失败')
        return jsonify(error='保存影片信息失败'), 500
    if jobs or 'series' in body:
        from core.service_manager import ensure_service
        ensure_service('consumer')
    return jsonify({'new_code': code, 'rename_job_ids': jobs, **get_video_by_code(code)})


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


def _replacement_filename(video, source_name, classification, uncensored, part):
    extension = Path(source_name).suffix.lower()
    if extension not in VIDEO_EXTENSIONS:
        raise ValueError('不支持的影片扩展名')
    if part and not re.fullmatch(r'(?:part)?0*[1-9][0-9]*', part, re.IGNORECASE):
        raise ValueError('无效的分集编号')
    code = subtitle_code(video['code'], classification, uncensored=uncensored)
    actors = sorted(video.get('actors', []), key=lambda actor: actor.get('id', 0))
    record = VideoRecord(
        id=video.get('id', 0), code=code, part='part' + re.sub(r'(?i)^part', '', part) if part else None,
        original_file_path=Path('upload' + extension), title_zh_short=video.get('title_zh_short'),
        title_zh=video.get('title_zh'), title_jp=video.get('title_jp'),
        actors=[actor.get('name_zh') or actor['name'] for actor in actors],
    )
    return build_filename(record)


def _incoming_code(filename):
    return extract_video_code(Path(filename).stem)[0] or ''


def _replacement_classification(video, filename, requested, extra_tags=()):
    vr = any(tag['name'] == 'VR' for tag in video.get('tags', ())) or 'VR' in extra_tags
    if requested is not None:
        return validate_subtitle(requested, vr=vr)
    return NO_SUBTITLE if vr else default_subtitle(_incoming_code(filename))


@app.post('/api/videos/<string:code>/replace-preview')
def api_video_replace_preview(code: str):
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or not isinstance(payload.get('files'), list):
        return jsonify(error='缺少预览文件'), 400
    video = get_video_by_code(code)
    if not video or video.get('deleted'):
        return jsonify(error='video not found'), 404
    try:
        extra_tags = _parse_upload_tags(payload)
        validate_generic_tags(extra_tags)
        requested = validate_subtitle(payload['subtitle_classification']) if 'subtitle_classification' in payload else None
        names = [_replacement_filename(
            video, item['filename'], _replacement_classification(video, item['filename'], requested, extra_tags),
            payload.get('uncensored') is True if 'uncensored' in payload else
            _incoming_code(item['filename']).upper().endswith(('-U', '-UC')),
            item.get('part')) for item in payload['files']]
    except (ValueError, TypeError, KeyError) as exc:
        return jsonify(error=str(exc)), 400
    return jsonify(names=names)


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
        raw_classification = request.headers.get('X-Subtitle-Classification')
        subtitle_classification = unquote(raw_classification) if raw_classification is not None else None
        uncensored_leaked = request.headers.get('X-Uncensored-Leaked') == '1'
        uncensored_crack_tag = unquote(request.headers.get('X-Uncensored-Crack-Tag') or '') or None
        uncensored_native = request.headers.get('X-Uncensored-Native') == '1'
        if 'X-Uncensored-Leaked' not in request.headers and 'X-Uncensored-Native' not in request.headers:
            uncensored_leaked = _incoming_code(dest_name).upper().endswith(('-U', '-UC'))
        dest_part = (request.headers.get('X-Part') or '').strip() or None
        replace_whole = request.headers.get('X-Replace-Whole') == '1'
        additional_tags = _parse_upload_tags(None, unquote(request.headers.get('X-Tags') or ''))
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
        subtitle_classification = request.form.get('subtitle_classification')
        uncensored_leaked = request.form.get('uncensored_leaked') == '1'
        uncensored_crack_tag = request.form.get('uncensored_crack_tag') or None
        uncensored_native = request.form.get('uncensored_native') == '1'
        if 'uncensored_leaked' not in request.form and 'uncensored_native' not in request.form:
            uncensored_leaked = _incoming_code(dest_name).upper().endswith(('-U', '-UC'))
        dest_part = (request.form.get('part') or '').strip() or None
        replace_whole = request.form.get('replace_whole') == '1'
        additional_tags = _parse_upload_tags(request.form)
        body_stream = f.stream

    crack_tags = {'JAV 无码破解', 'LADA 无码破解', 'LTX 无码破解'}
    if uncensored_crack_tag is not None and uncensored_crack_tag not in crack_tags:
        return jsonify({'error': 'invalid uncensored_crack_tag'}), 400
    try:
        validate_generic_tags(additional_tags)
        if subtitle_classification is not None:
            validate_subtitle(subtitle_classification)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    missing_tags = _missing_existing_tags(additional_tags)
    if missing_tags:
        return jsonify({'error': 'unknown tags', 'tags': missing_tags}), 400

    if not dest_name:
        return jsonify({'error': 'missing dest_name'}), 400
    if not expected_size:
        return jsonify({'error': 'file_size must be positive'}), 400
    original_path = get_video_file_path(code, original_part)
    if not original_path:
        return jsonify({'error': 'video not found'}), 404
    original = Path(original_path)
    video = get_video_by_code(code)
    if not video or video.get('deleted'):
        return jsonify({'error': 'video not found'}), 404
    part = None if replace_whole else dest_part or original_part
    try:
        subtitle_classification = _replacement_classification(video, dest_name, subtitle_classification, additional_tags)
        subtitle_zh = subtitle_classification != NO_SUBTITLE
        final_name = _replacement_filename(video, dest_name, subtitle_classification, uncensored_leaked or uncensored_native, part)
        identity = original.stat()
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except FileNotFoundError:
        return jsonify(error='file not found'), 404
    dest_path = original.parent / final_name
    if dest_path != original and (dest_path.exists() or dest_path.is_symlink()):
        return jsonify(error='目标文件已存在，已保留原文件'), 409

    temp_path = backup = None
    installed = False

    def install_file():
        nonlocal installed, backup
        current = original.stat()
        if (current.st_ino, current.st_size, current.st_mtime_ns) != (identity.st_ino, identity.st_size, identity.st_mtime_ns):
            raise FileExistsError('原文件已改变，请重新选择')
        if dest_path == original:
            backup_path = temp_path.with_suffix('.backup')
            original.rename(backup_path)
            backup = backup_path
        else:
            # Reserve the destination without clobbering an existing file, including on exFAT.
            with dest_path.open('xb'):
                installed = True
        temp_path.replace(dest_path)
        installed = True

    try:
        written = 0
        with tempfile.NamedTemporaryFile(dir=original.parent, suffix='.temp', delete=False) as dst:
            temp_path = Path(dst.name)
            while chunk := body_stream.read(4 * 1024 * 1024):
                dst.write(chunk)
                written += len(chunk)
        if written != expected_size:
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
        crack_kwargs = (
            {'uncensored_crack_tag': uncensored_crack_tag}
            if uncensored_crack_tag is not None else {}
        )
        if subtitle_classification is not None:
            crack_kwargs['subtitle_classification'] = subtitle_classification
        if replace_whole:
            result = replace_multipart_with_single_file(
                code, str(dest_path), subtitle_zh, uncensored_leaked,
                uncensored_native, file_meta=file_meta, keep_part=original_part,
                before_commit=install_file, expected_path=original_path, **crack_kwargs,
            )
        else:
            result = replace_video_file(
                code, original_part, str(dest_path), subtitle_zh, uncensored_leaked,
                uncensored_native, file_meta=file_meta, before_commit=install_file, expected_path=original_path,
                new_part='part' + re.sub(r'(?i)^part', '', part) if part else None, **crack_kwargs,
            )
        if not result['ok'] or not installed:
            raise RuntimeError(result.get('error') or 'db update failed')
    except (OSError, RuntimeError, ValueError) as exc:
        if backup is not None:
            backup.replace(original)
        elif installed:
            dest_path.unlink(missing_ok=True)
        return jsonify(error=str(exc)), 409 if isinstance(exc, FileExistsError) else 400 if isinstance(exc, ValueError) else 500
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    old_paths = result.get('old_paths', [original_path])
    for old_path in [*map(Path, old_paths), backup]:
        if old_path is not None and old_path != dest_path:
            try:
                old_path.unlink(missing_ok=True)
            except OSError:
                logging.exception('替换已完成，旧文件清理失败：%s', old_path)

    # replace_video_file may re-suffix the code (e.g. -C → -UC), so fetch the
    # detail by the new code, not the original request code.
    final_code = result.get('new_code') or code
    if additional_tags and not add_existing_tags_to_video(final_code, additional_tags):
        return jsonify({'error': 'failed to add tags'}), 500
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

    video = get_video_by_code(code)
    if not video or video.get('deleted'):
        return jsonify({'error': 'not found'}), 404
    classification = video.get('subtitle_classification')
    if classification and subtitle_code(new_code, classification) != new_code:
        return jsonify(error='番号后缀与字幕分类不一致'), 400
    if not old_path.exists():
        return jsonify({'error': 'source file missing'}), 400
    if new_code == video['code'] and new_path != old_path and new_path.exists() and not old_path.samefile(new_path):
        return jsonify({'error': 'target exists'}), 409
    if new_path == old_path and new_code == video['code']:
        return jsonify({'ok': True, 'rename_job_ids': [], **video})
    try:
        new_code, jobs = patch_video(code, {}, runtime=scrape_store(), new_code=new_code,
                                     manual_name=(str(old_path), new_name))
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except sqlite3.Error:
        app.logger.exception('登记文件重命名失败')
        return jsonify(error='登记文件重命名失败'), 500
    from core.service_manager import ensure_service
    ensure_service('consumer')
    return jsonify({'ok': True, 'rename_job_ids': jobs, **get_video_by_code(new_code)})


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
    resp = send_file(
        path, mimetype=mime, conditional=True,
        as_attachment=request.args.get('download') == '1',
        download_name=Path(path).name,
    )
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


def _subtitle_identity(code, part):
    path = get_video_file_path(code, part)
    if not path or Path(path).suffix.lower() != '.mkv':
        return None, None
    try:
        return path, subtitle_cache.fingerprint(path)
    except OSError:
        return None, None


@app.route('/api/videos/<string:code>/subtitles', methods=['GET', 'POST'])
def api_video_subtitles(code: str):
    part = request.args.get('part') or None
    path, key = _subtitle_identity(code, part)
    if not key:
        return jsonify(tracks=[])
    tracks = subtitle_cache.manifest(key)
    if tracks is not None:
        return jsonify(tracks=[{**track, 'url': url_for('api_video_subtitle_track',
            code=code, key=key, track_id=track['id'], part=part)} for track in tracks])
    runtime = scrape_store()
    task_id = 'subtitle-' + key
    task = runtime.get(task_id)
    if task and task['status'] in ('FAILED', 'SKIPPED', 'CANCELLED', 'DONE'):
        return jsonify(error=task['error'] or '字幕缓存不可用'), 503
    if request.method == 'POST':
        if not task:
            runtime.submit('subtitle_cache', code, {'path': path, 'fingerprint': key},
                           task_id=task_id, priority=-1500)
        from core.service_manager import ensure_service
        ensure_service('consumer')
    return jsonify(status='pending'), 202


@app.get('/api/videos/<string:code>/subtitles/<string:key>/<int:track_id>.vtt')
def api_video_subtitle_track(code: str, key: str, track_id: int):
    _, current = _subtitle_identity(code, request.args.get('part') or None)
    if key != current:
        return jsonify(error='字幕已失效'), 404
    tracks = subtitle_cache.manifest(key)
    if tracks is None or not any(track['id'] == track_id for track in tracks):
        return jsonify(error='未找到字幕轨'), 404
    return send_file(subtitle_cache.CACHE_ROOT / key / f'{track_id}.vtt', mimetype='text/vtt',
                     conditional=True, max_age=3600)


@app.get('/api/videos/<string:code>/playback')
def api_playback_history(code: str):
    if not get_video_file_path(code):
        return jsonify(error='video not found'), 404
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        return jsonify(playback.history(conn, code))


@app.post('/api/videos/<string:code>/playback/<string:session_id>')
def api_playback_report(code: str, session_id: str):
    try:
        payload = request.get_json(silent=True)
        if payload is None and request.form.get('source') == 'iina':
            payload = {'source': 'iina', 'part': request.form.get('part') or None}
            for name in ('started_at', 'last_played_at', 'watched_ms', 'position_ms', 'sequence'):
                payload[name] = int(request.form.get(name, ''))
        with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn, conn:
            conn.row_factory = sqlite3.Row
            conn.execute('BEGIN IMMEDIATE')
            if not get_video_file_path(code, connection=conn):
                return jsonify(error='video not found'), 404
            playback.report(conn, code, session_id, payload)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    return '', 204


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
    return jsonify(get_recommendations(count=8, tags=_selected_tags(),
                                       exclude_tags=request.args.get('exclude') == '1'))


@app.get('/api/tags/all')
def api_tags_all():
    return jsonify(list_all_tags_with_count(include_unused=request.args.get('include_unused') == '1'))


@app.get('/api/actors/all')
def api_actors_all():
    return jsonify(list_all_actors_with_count())


@app.get('/api/series/all')
def api_series_all():
    return jsonify(list_all_series_with_count())


@app.get('/api/series/videos')
def api_series_videos():
    jp_id = request.args.get('jp', type=int)
    final_id = request.args.get('final', type=int)
    s = request.args.get('s') or None
    if sum(bool(key) for key in (jp_id, final_id, s)) != 1:
        return jsonify({'error': '请指定一个系列'}), 400
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    sort = request.args.get('sort', 'date')
    return jsonify(get_videos_by_series(series_name=s, jp_id=jp_id, final_id=final_id, page=page, limit=limit, sort=sort))


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
    if not isinstance(body, dict):
        return jsonify({'error': 'no valid fields or not found'}), 400
    runtime = scrape_store()
    rename_jobs = []

    def enqueue_rename(conn, code, video_ids):
        rename_jobs.append(runtime.submit('rename_files', code, {'video_ids': video_ids}, connection=conn))

    target_id = patch_actor(actor_id, body, enqueue_rename=enqueue_rename)
    if target_id is None:
        return jsonify({'error': 'no valid fields or not found'}), 400
    if rename_jobs:
        from core.service_manager import ensure_service
        ensure_service('consumer')
    data = get_actor_with_videos(target_id)
    result = {'merged': target_id != actor_id, 'actor': data['actor'], 'rename_job_ids': rename_jobs}
    if target_id != actor_id:
        result['redirect_id'] = target_id
    return jsonify(result)


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

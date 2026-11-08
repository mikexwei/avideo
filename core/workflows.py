"""Local workflow stages. External work is submitted by the Consumer."""
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import DB_PATH
from core.scanner import extract_video_code
from core.task_policy import is_special_code
from dal.db_manager import (batch_insert_scanned_videos, sync_derived_video_tags_for_code,
                            add_existing_tags_to_video, get_video_by_code, update_video_metadata)
from utils.rename_and_move_videos import VideoRecord, build_destination, build_filename


def prepare_upload(job, file_paths, volume_path, uncensored_crack_tag='LTX 无码破解',
                   subtitle_classification=None, additional_tags=None):
    def emit(step, status, message, **extra):
        job.put({'step': step, 'status': status, 'message': message, **extra})
    # ── Step 1: Parse codes ──────────────────────────────────
    sorted_paths = sorted(file_paths, key=lambda p: p.name)
    codes_parts = []
    for i, fpath in enumerate(sorted_paths):
        code, part = extract_video_code(fpath.stem)
        if not code:
            raise ValueError(f'无法解析番号: {fpath.name}，请改用手动模式')
        if is_special_code(code):
            raise ValueError('CN 和 DUMMY 请使用手动上传')
        if subtitle_classification is not None:
            base = re.sub(r'(?i)-(?:uc|u|c)$', '', code)
            has_u = bool(uncensored_crack_tag) or code.upper().endswith(('-U', '-UC'))
            has_c = bool(subtitle_classification)
            code = base + ('-UC' if has_u and has_c else '-U' if has_u else '-C' if has_c else '')
        if len(sorted_paths) > 1 and not part:
            part = f'part{i + 1}'
        codes_parts.append((code, part, fpath))
    main_code = codes_parts[0][0]
    emit('parse_code', 'done', f'番号: {main_code}')

    # ── Steps 2-3: Move to destination ──────────────────────
    dest_dir = None
    scanned = []
    checkpoint = job.checkpoint() if hasattr(job, 'checkpoint') else {}
    planned = checkpoint.get('upload_files')
    if planned is None:
        planned = []
        for code, part, tmp_path in codes_parts:
            record = VideoRecord(id=0, code=code, part=part, original_file_path=tmp_path,
                                 title_zh_short=None, title_zh=None, title_jp=None, actors=[])
            target = build_destination(record, dest_root=volume_path)
            st = tmp_path.stat()
            planned.append({'code': code, 'part': part, 'source': str(tmp_path), 'original_path': str(target),
                            'file_size': st.st_size, 'file_mtime': datetime.fromtimestamp(st.st_mtime).isoformat(),
                            'file_birthtime': datetime.fromtimestamp(getattr(st, 'st_birthtime', st.st_mtime)).isoformat()})
        if hasattr(job, 'checkpoint'):
            checkpoint['upload_files'] = planned
            job.checkpoint(checkpoint)
    for item in planned:
        code, part, tmp_path = item['code'], item['part'], Path(item['source'])
        dest_path = Path(item['original_path'])
        dest_dir = dest_path.parent
        dest_dir.mkdir(parents=True, exist_ok=True)
        if tmp_path.exists():
            if dest_path.exists() and tmp_path != dest_path:
                raise ValueError(f'目标文件已存在，保留上传文件: {dest_path}')
            tmp_path.rename(dest_path)
        elif not dest_path.exists():
            with sqlite3.connect(DB_PATH) as recovery:
                row = recovery.execute('SELECT original_file_path FROM videos WHERE UPPER(code)=UPPER(?) AND COALESCE(part,\'\')=COALESCE(?,\'\') AND deleted=0 LIMIT 1', (code, part)).fetchone()
            if not row or not Path(row[0]).exists():
                raise FileNotFoundError(f'上传文件和目标文件均不存在: {tmp_path}')
            dest_path = Path(row[0])
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
    sync_derived_video_tags_for_code(main_code, uncensored_crack_tag, subtitle_classification)
    if additional_tags and not add_existing_tags_to_video(main_code, additional_tags):
        raise ValueError('添加标签失败')
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

    return main_code


def video_file_record(conn, row):
    actors = [item[0] for item in conn.execute(
        "SELECT COALESCE(NULLIF(a.name_zh, ''), a.name) FROM actors a "
        "JOIN video_actor_link l ON l.actor_id=a.id WHERE l.video_id=? ORDER BY a.id",
        (row['id'],)) if item[0]]
    return VideoRecord(id=row['id'], code=row['code'], part=row['part'],
        original_file_path=Path(row['original_file_path']), title_zh_short=row['title_zh_short'],
        title_zh=row['title_zh'], title_jp=row['title_jp'], actors=actors)


def _rename_files_for_code(main_code: str, job=None, *, video_ids=None, move_to_prefix=False, manual_name=None) -> int:
    """Journal each move before touching files, then serialize local DB changes."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    changed = 0

    def destination(record):
        if video_ids is None or move_to_prefix:
            target = _build_current_library_destination(record)
        else:
            target = record.original_file_path.with_name(build_filename(record))
        if (manual_name and record.id == manual_name['video_id']
                and target.name == manual_name['canonical']):
            target = target.with_name(manual_name['name'])
        return target

    def check_destination(actual, target):
        if actual != target and (target.exists() or target.is_symlink()):
            same_casefolded_path = str(actual).casefold() == str(target).casefold()
            if not (same_casefolded_path and not target.is_symlink() and actual.samefile(target)):
                raise FileExistsError(f'目标文件已存在，保留原文件 {target}')

    try:
        if video_ids is None:
            rows = conn.execute('SELECT * FROM videos WHERE UPPER(code)=UPPER(?) AND deleted=0 ORDER BY id', (main_code,)).fetchall()
        else:
            rows = conn.execute("""SELECT * FROM videos WHERE deleted=0
                AND id IN (SELECT value FROM json_each(?)) ORDER BY id""", (json.dumps(video_ids),)).fetchall()
        for row in rows:
            record = video_file_record(conn, row)
            if is_special_code(record.code):
                continue
            source = record.original_file_path
            target = destination(record)
            if target == source and source.is_file():
                continue
            checkpoint = job.checkpoint() if job else {}
            recovery = checkpoint.get('rename_moves', {}).get(str(source))
            identity = checkpoint.get('rename_identity', {}).get(str(source))
            actual = source
            if not source.exists() and recovery:
                previous = checkpoint.get('rename_previous', {}).get(str(source))
                for candidate in filter(None, (recovery, previous)):
                    recovered = Path(candidate)
                    if recovered.is_file():
                        stat = recovered.stat()
                        if not identity or [stat.st_ino, stat.st_size, stat.st_mtime_ns] == identity:
                            actual = recovered
                            break
                if actual == source and Path(recovery).exists():
                    raise ValueError(f'恢复目标与原文件不一致 {recovery}')
            if not actual.is_file():
                raise FileNotFoundError(str(source))
            check_destination(actual, target)
            stat = actual.stat()
            if actual != target and job:
                checkpoint.setdefault('rename_moves', {})[str(source)] = str(target)
                checkpoint.setdefault('rename_identity', {})[str(source)] = [stat.st_ino, stat.st_size, stat.st_mtime_ns]
                if actual != source:
                    checkpoint.setdefault('rename_previous', {})[str(source)] = str(actual)
                job.checkpoint(checkpoint)
            conn.execute('BEGIN IMMEDIATE')
            current = conn.execute('SELECT * FROM videos WHERE id=? AND deleted=0', (row['id'],)).fetchone()
            if current is None or dict(current) != dict(row):
                raise ValueError('影片在文件重命名前已改变')
            if destination(video_file_record(conn, current)) != target:
                raise ValueError('影片命名信息已再次改变，请重试')
            if job:
                task = conn.execute('SELECT status,cancel_requested,execution_id FROM scrape_tasks WHERE id=?', (job.job_id,)).fetchone()
                if task is None or task['cancel_requested'] or task['status'] == 'CANCELLED' or (job.execution_id is not None and task['execution_id'] != job.execution_id):
                    raise ValueError('任务已取消')
            current_stat = actual.stat()
            if (current_stat.st_ino, current_stat.st_size, current_stat.st_mtime_ns) != (
                    stat.st_ino, stat.st_size, stat.st_mtime_ns):
                raise ValueError('原文件已改变，请重试')
            if actual != target:
                if conn.execute('SELECT 1 FROM videos WHERE original_file_path=? AND id<>?',
                                (str(target), row['id'])).fetchone():
                    raise FileExistsError(f'数据库已登记目标路径 {target}')
                check_destination(actual, target)
                target.parent.mkdir(parents=True, exist_ok=True)
                actual.rename(target)
            conn.execute('UPDATE videos SET original_file_path=?,updated_at=CURRENT_TIMESTAMP WHERE id=?',
                         (str(target), row['id']))
            conn.commit()
            changed += source != target
        return changed
    finally:
        conn.close()


def _library_root_for_path(path: Path) -> Optional[Path]:
    for parent in path.parents:
        if parent.name.lower() == 'avideo':
            return parent
    return None


def _build_current_library_destination(record: VideoRecord) -> Path:
    if is_special_code(record.code):
        return record.original_file_path
    library_root = _library_root_for_path(record.original_file_path)
    if library_root is None:
        return record.original_file_path.parent / build_filename(record)
    return build_destination(record, dest_root=library_root)

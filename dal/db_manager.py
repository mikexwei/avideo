from contextlib import closing
import re
import sqlite3
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Any

from zhconv import convert

# 为了解决模块导入路径问题，动态引入 config
import sys
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH
from dal.playback import VIDEO_KEY_SQL, ensure_schema as ensure_playback_schema

logger = logging.getLogger(__name__)


def _to_simplified(value: Any) -> Any:
    return convert(value, 'zh-cn') if isinstance(value, str) and value else value


def _actor_name_key(name):
    return _to_simplified(name.strip()) if name else ''


def _actor_by_name(cursor, name, exclude_id=0, *, include_display=False):
    key = _actor_name_key(name)
    exact_display = None
    equivalent = None
    columns = 'id,name,is_ignored,name_zh' if include_display else 'id,name,is_ignored'
    for row in cursor.execute(f"SELECT {columns} FROM actors WHERE id != ? ORDER BY id", (exclude_id,)):
        if row[1] == key:
            return row
        if equivalent is None and _actor_name_key(row[1]) == key:
            equivalent = row
        if include_display and exact_display is None and row[3] == name:
            exact_display = row
    return equivalent if equivalent is not None else exact_display


def _get_or_create_actor(cursor, name, name_zh=''):
    actor = _actor_by_name(cursor, name)
    if actor is None:
        cursor.execute("INSERT INTO actors (name) VALUES (?)", (_actor_name_key(name),))
        actor_id, ignored, created = cursor.lastrowid, False, True
    else:
        actor_id, ignored, created = actor[0], actor[2], False
    if name_zh:
        cursor.execute("UPDATE actors SET name_zh = ? WHERE id = ? AND COALESCE(name_zh,'')=''",
                       (_actor_name_key(name_zh), actor_id))
    return actor_id, ignored, created


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL reuses a single -wal file instead of creating a per-transaction journal,
    # which is far more robust when the disk is low on space or under concurrency.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]

def _order_clause(sort: str) -> str:
    """Return the ORDER BY body for video queries."""
    if sort == 'score':
        return "COALESCE(score, 0) DESC, COALESCE(release_date, '') DESC, id DESC"
    if sort == 'mtime':
        return "COALESCE(file_mtime, '') DESC, id DESC"
    return "COALESCE(release_date, '') DESC, id DESC"


_VERSION_SUFFIXES = ("-UC", "-C", "-U")
_UNCENSORED_CRACK_TAGS = ("JAV 无码破解", "LADA 无码破解", "LTX 无码破解")
_LEGACY_UNCENSORED_CRACK_TAGS = ("无码破解", "無碼破解")
_DERIVED_TAGS = ("无码流出",) + _UNCENSORED_CRACK_TAGS + ("原装无码", "中文字幕", "机翻中文")
_DERIVED_TAGS_TO_REMOVE = _DERIVED_TAGS + ("無碼流出",) + _LEGACY_UNCENSORED_CRACK_TAGS
_ORIGINAL_UNCENSORED_MAKERS = {
    "一本道",
    "HEYZO",
    "Gachinco",
    "カリビアンコム",
    "Heydouga",
    "G-Queen",
    "Hunter",
    "Tokyo-Hot",
    "ラフォーレガール",
    "スーパーモデルメディア",
}


def _strip_version_suffix(code: str) -> str:
    upper = (code or "").upper()
    for suffix in _VERSION_SUFFIXES:
        if upper.endswith(suffix):
            return code[: -len(suffix)]
    return code


def _resolve_video_code(cursor: sqlite3.Cursor, code: str) -> Optional[str]:
    """Resolve a user-facing base code to the stored code variant, preserving exact matches."""
    if not code:
        return None

    cursor.execute("SELECT code FROM videos WHERE UPPER(code) = UPPER(?) ORDER BY id ASC LIMIT 1", (code,))
    row = cursor.fetchone()
    if row:
        return row["code"]

    base = _strip_version_suffix(code)
    if base != code:
        return None

    for candidate in (f"{base}-UC", f"{base}-C", f"{base}-U"):
        cursor.execute("SELECT code FROM videos WHERE UPPER(code) = UPPER(?) ORDER BY id ASC LIMIT 1", (candidate,))
        row = cursor.fetchone()
        if row:
            return row["code"]
    return None


def _version_suffix(code: str) -> str:
    upper = (code or "").upper()
    for suffix in _VERSION_SUFFIXES:
        if upper.endswith(suffix):
            return suffix
    return ""


def _tag_filter_conditions(alias: str, tags: Optional[List[str]]) -> tuple[list[str], list[str]]:
    conditions = []
    params = []
    for tag in tags or []:
        tag = tag.strip()
        if not tag:
            continue
        conditions.append(
            f"""EXISTS (
                SELECT 1
                FROM video_tag_link vtl_filter
                JOIN tags t_filter ON t_filter.id = vtl_filter.tag_id
                WHERE vtl_filter.video_id = {alias}.id
                  AND t_filter.name = ?
            )"""
        )
        params.append(tag)
    return conditions, params

def batch_insert_scanned_videos(scanned_results: list, scrape_status: str = 'PENDING') -> tuple[int, int]:
    """
    将扫描器提取的结果批量安全地存入数据库。
    返回: (新增插入的数量, 因为已存在而跳过的数量)
    """
    if not scanned_results:
        return 0, 0

    inserted_count = 0
    skipped_count = 0
    
    # 我们只挑选提取出有效 code 的视频入库
    valid_items = [item for item in scanned_results if item.get('code')]

    if not valid_items:
        return 0, 0

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        # 每次连接数据库都必须开启外键约束，确保 ON DELETE CASCADE 生效
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()

        # 核心 SQL: 使用 INSERT OR IGNORE
        # 如果这个物理路径 (original_file_path) 已经在库里了，就静默跳过，不会报错
        insert_sql = """
            INSERT OR IGNORE INTO videos
            (code, part, original_file_path, scrape_status, file_size, file_mtime, file_birthtime)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        for item in valid_items:
            # 提取数据准备插入
            code = item['code']
            part = item.get('part')
            # 必须转成字符串存入数据库
            file_path_str = str(item['original_path'])

            cursor.execute(insert_sql, (
                code, part, file_path_str, scrape_status,
                item.get('file_size'), item.get('file_mtime'), item.get('file_birthtime'),
            ))
            
            # rowcount 为 1 表示插入成功，为 0 表示因为 UNIQUE 约束被 IGNORE 了
            if cursor.rowcount == 1:
                inserted_count += 1
            else:
                skipped_count += 1

        conn.commit()
        logger.info(f"💾 入库完毕: 新增 {inserted_count} 条，跳过已存在 {skipped_count} 条。")
        
        return inserted_count, skipped_count

    except sqlite3.Error as e:
        logger.error(f"❌ 数据库批量插入失败: {e}")
        return 0, 0
    finally:
        if conn:
            conn.close()

def get_pending_videos(limit: int = 5, status: str = 'PENDING') -> List[Tuple[int, str]]:
    """
    从数据库中捞取指定数量的待刮削影片。
    :param status: 'PENDING' 或 'FAILED'
    返回格式: [(id, code), (id, code)...]
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        sql = "SELECT id, code FROM videos WHERE scrape_status = ? ORDER BY created_at ASC LIMIT ?"
        cursor.execute(sql, (status.upper(), limit))
        results = cursor.fetchall()

        return results
    except sqlite3.Error as e:
        logger.error(f"❌ 捞取 PENDING 任务失败: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_video_metadata(video_id: int, status: str, data: Optional[Dict] = None, *, connection=None):
    """
    保存刮削结果及演员、标签关联，并返回新建的演员 ID 和姓名。
    """
    conn = connection
    owns_connection = connection is None
    new_actors = []
    try:
        if owns_connection:
            conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 0. 获取与此 video_id 关联的番号 (code)，并找出所有拥有此 code 的视频 ID
        cursor.execute("SELECT code FROM videos WHERE id = ?", (video_id,))
        row = cursor.fetchone()
        if not row:
            logger.warning(f"⚠️ 未找到 ID 为 {video_id} 的视频记录。")
            return new_actors
        
        code = row[0]
        cursor.execute("SELECT id FROM videos WHERE code = ?", (code,))
        target_video_ids = [r[0] for r in cursor.fetchall()]
        
        if status == 'SUCCESS' and data:
            series_value = (data.get('series') or '').strip()

            # 1. 批量更新 videos 主表中所有具有相同 code 的记录
            sql_video = """
                UPDATE videos 
                SET title_jp = ?, release_date = ?, duration = ?, 
                    maker = ?, publisher = ?, series = ?, score = ?, 
                    cover_path = ?, scrape_status = 'SUCCESS', updated_at = CURRENT_TIMESTAMP
                WHERE code = ?
            """
            cursor.execute(sql_video, (
                data.get('title_jp', ''), data.get('release_date', ''), data.get('duration', ''),
                _to_simplified(data.get('maker', '')),
                _to_simplified(data.get('publisher', '')), series_value,
                data.get('score', 0.0), data.get('cover_path', ''), code
            ))
            
            # 2. 处理演员 (Actors) 的多对多入库
            placeholders = ','.join('?' * len(target_video_ids))
            cursor.execute(
                f"DELETE FROM video_actor_link WHERE video_id IN ({placeholders})",
                target_video_ids,
            )
            actors_str = data.get('actors', '')
            if actors_str:
                # 把逗号分隔的字符串拆成列表，并去掉两边空格
                actor_list = dict.fromkeys(_actor_name_key(a) for a in actors_str.split(',') if a.strip())
                for actor_name in actor_list:
                    actor_id, ignored, actor_created = _get_or_create_actor(cursor, actor_name)
                    if not ignored:
                        if actor_created:
                            new_actors.append((actor_id, actor_name))
                        # 为所有具有相同 code 的 video_id 插入关联表
                        for t_v_id in target_video_ids:
                            cursor.execute("INSERT OR IGNORE INTO video_actor_link (video_id, actor_id) VALUES (?, ?)", (t_v_id, actor_id))

            # 3. 处理标签/类别 (Tags/Categories) 的多对多入库
            tags_str = data.get('categories', '')
            if tags_str:
                tag_list = [_to_simplified(t.strip()) for t in tags_str.split(',') if t.strip()]
                for tag_name in tag_list:
                    cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
                    cursor.execute("SELECT id FROM tags WHERE name = ?", (tag_name,))
                    tag_row = cursor.fetchone()
                    if tag_row:
                        tag_id = tag_row[0]
                        for t_v_id in target_video_ids:
                            cursor.execute("INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)", (t_v_id, tag_id))
            
        else:
            # 非成功状态（FAILED / FREEZE 等），批量更新所有相同 code 的状态
            new_status = status if status in ('FAILED', 'FREEZE', 'PENDING') else 'FAILED'
            sql_fail = "UPDATE videos SET scrape_status = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?"
            cursor.execute(sql_fail, (new_status, code))
            
        # 所有操作要么一起成功，要么一起失败 (事务保障)
        if owns_connection:
            conn.commit()
        if status == 'SUCCESS' and data:
            sync_derived_video_tags_for_code(code, connection=connection)
        logger.debug(f"💾 数据库多表联动更新完毕: 番号 {code} (包含 {len(target_video_ids)} 个分集记录) -> 状态 {status}")
        return new_actors
        
    except sqlite3.Error as e:
        logger.error(f"❌ 数据库多表更新失败 (ID: {video_id}): {e}")
        if not owns_connection:
            raise
        # 如果发生异常，数据库会自动回滚，防止产生脏数据
    finally:
        if conn and owns_connection:
            conn.close()


def _uncensored_crack_tag(file_birthtime: Optional[str]) -> str:
    date = (file_birthtime or "")[:10]
    if date >= "2026-10-10":
        return "LTX 无码破解"
    if date >= "2025-11-10":
        return "LADA 无码破解"
    return "JAV 无码破解"


def _derived_tags_for_video(
    code: str,
    maker: str,
    existing_tags: set[str],
    file_birthtime: Optional[str] = None,
    uncensored_crack_tag: Optional[str] = None,
    subtitle_classification: Optional[str] = None,
) -> list[str]:
    tags = []
    suffix = _version_suffix(code)

    if "无码流出" in existing_tags or "無碼流出" in existing_tags:
        tags.append("无码流出")
    elif uncensored_crack_tag == "":
        pass
    elif uncensored_crack_tag in _UNCENSORED_CRACK_TAGS:
        tags.append(uncensored_crack_tag)
    elif existing_tags.intersection(_UNCENSORED_CRACK_TAGS + _LEGACY_UNCENSORED_CRACK_TAGS) or suffix in {"-U", "-UC"}:
        tags.append(_uncensored_crack_tag(file_birthtime))
    elif maker in _ORIGINAL_UNCENSORED_MAKERS:
        tags.append("原装无码")

    if subtitle_classification == "":
        pass
    elif subtitle_classification:
        tags.append(subtitle_classification)
    elif "机翻中文" in existing_tags:
        tags.append("机翻中文")
    elif suffix in {"-C", "-UC"}:
        tags.append("中文字幕")
    return tags


def sync_derived_video_tags_for_code(
    code: str,
    uncensored_crack_tag: Optional[str] = None,
    subtitle_classification: Optional[str] = None,
    *, connection=None,
) -> int:
    conn = connection
    owns_connection = connection is None
    try:
        if owns_connection:
            conn = _get_conn()
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        cursor.execute("SELECT id, code, maker, file_birthtime, file_mtime FROM videos WHERE code = ?", (code,))
        video_rows = cursor.fetchall()
        if not video_rows:
            return 0

        video_ids = [r["id"] for r in video_rows]
        id_placeholders = ",".join("?" * len(video_ids))
        cursor.execute(
            f"""
            SELECT DISTINCT t.name
            FROM tags t
            JOIN video_tag_link vtl ON vtl.tag_id = t.id
            WHERE vtl.video_id IN ({id_placeholders})
            """,
            video_ids,
        )
        existing_tags = {r["name"] for r in cursor.fetchall()}

        cursor.execute(
            f"""
            DELETE FROM video_tag_link
            WHERE video_id IN ({id_placeholders})
              AND tag_id IN (SELECT id FROM tags WHERE name IN ({",".join("?" * len(_DERIVED_TAGS_TO_REMOVE))}))
            """,
            video_ids + list(_DERIVED_TAGS_TO_REMOVE),
        )

        changed = 0
        for video_row in video_rows:
            derived_tags = _derived_tags_for_video(
                video_row["code"], video_row["maker"] or "", existing_tags,
                video_row["file_birthtime"] or video_row["file_mtime"],
                uncensored_crack_tag,
                subtitle_classification,
            )
            for tag in derived_tags:
                cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
                cursor.execute("SELECT id FROM tags WHERE name = ?", (tag,))
                tag_id = cursor.fetchone()["id"]
                cursor.execute(
                    "INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)",
                    (video_row["id"], tag_id),
                )
                changed += cursor.rowcount

        if owns_connection:
            conn.commit()
        return changed
    except sqlite3.Error as e:
        logger.error(f"❌ sync_derived_video_tags_for_code 失败 [{code}]: {e}")
        if not owns_connection:
            raise
        return 0
    finally:
        if conn and owns_connection:
            conn.close()


def sync_all_derived_video_tags() -> int:
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT code FROM videos")
        codes = [r["code"] for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"❌ sync_all_derived_video_tags 读取失败: {e}")
        return 0
    finally:
        if conn:
            conn.close()

    changed = 0
    for code in codes:
        changed += sync_derived_video_tags_for_code(code)
    return changed

def get_pending_actors(limit: int = 10) -> List[Tuple[int, str]]:
    """捞取还没有头像且未被忽略的演员 (仅限女优)"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # avatar_path 为空或者是特殊标记 'no_avatar' 以外的，都抓取
        sql = "SELECT id, name FROM actors WHERE is_ignored = 0 AND (avatar_path IS NULL OR avatar_path = '') LIMIT ?"
        cursor.execute(sql, (limit,))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"❌ 捞取待刮削演员失败: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_actor_avatar(actor_id: int, avatar_path: Optional[str], actor_name: str = "", name_zh: Optional[str] = None, *, connection=None):
    """更新演员头像路径，如果发现同图，则执行自动合并归一化"""
    conn = connection
    owns_connection = connection is None
    try:
        if owns_connection:
            conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        
        # 自动无感升级：如果 actors 表还没有 name_zh 列，则动态添加
        try:
            cursor.execute("ALTER TABLE actors ADD COLUMN name_zh TEXT")
        except sqlite3.OperationalError:
            pass  # 如果列已存在，则会抛出此异常，直接跳过即可
            
        # 如果抓不到，写入 'no_avatar' 以防下次循环死磕它
        path_to_save = avatar_path if avatar_path else 'no_avatar'
        
        if path_to_save != 'no_avatar':
            # 查找是否有其他女优已经使用了这个完全一样的头像路径
            cursor.execute("SELECT id, name FROM actors WHERE avatar_path = ? AND id != ?", (path_to_save, actor_id))
            duplicate = cursor.fetchone()
            
            if duplicate:
                target_id, target_name = duplicate
                logger.warning(f"👯 发现马甲/重名！[{actor_name}] 的头像与 [{target_name}] 完全一致。正在执行自动合并...")
                
                # 1. 将属于旧演员的视频关联，全部转移给主演员 (使用 OR IGNORE 防止同一个视频同时有这两个名字导致主键冲突)
                cursor.execute("UPDATE OR IGNORE video_actor_link SET actor_id = ? WHERE actor_id = ?", (target_id, actor_id))
                
                # 2. 清理可能因为 IGNORE 遗留的旧关联
                cursor.execute("DELETE FROM video_actor_link WHERE actor_id = ?", (actor_id,))
                
                # 3. 删除这个冗余的马甲记录
                cursor.execute("DELETE FROM actors WHERE id = ?", (actor_id,))
                if owns_connection:
                    conn.commit()
                logger.info(f"✅ 合并完毕: [{actor_name}] 的所有作品已归入 [{target_name}] 旗下。")
                return
                
        # 更新数据库
        if name_zh:
            cursor.execute("UPDATE actors SET avatar_path = ?, name_zh = ? WHERE id = ?", (path_to_save, name_zh, actor_id))
        else:
            cursor.execute("UPDATE actors SET avatar_path = ? WHERE id = ?", (path_to_save, actor_id))
        if owns_connection:
            conn.commit()
        
        log_msg = f"💾 演员信息入库完毕 (ID: {actor_id}) -> 头像: {path_to_save}" + (f" | 译名: {name_zh}" if name_zh else "")
        logger.info(log_msg)
    except sqlite3.Error as e:
        logger.error(f"❌ 演员头像更新失败 (ID: {actor_id}): {e}")
        if not owns_connection:
            raise
    finally:
        if conn and owns_connection:
            conn.close()


def list_videos(
    page: int = 1,
    limit: int = 24,
    sort: str = 'date',
    year: str = None,
    score_min: float = None,
    score_max: float = None,
    has_cover: bool = False,
    has_translation: bool = False,
    scrape_status: str = None,
    tags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    page = max(1, page)
    limit = max(1, min(limit, 200))
    offset = (page - 1) * limit

    # Build dynamic WHERE clauses
    conditions = ["v.deleted = 0"]
    params: list = []
    if year:
        conditions.append("substr(v.release_date, 1, 4) = ?")
        params.append(str(year))
    if score_min is not None:
        conditions.append("v.score >= ?")
        params.append(score_min)
    if score_max is not None:
        conditions.append("v.score < ?")
        params.append(score_max)
    if has_cover:
        conditions.append("v.cover_path IS NOT NULL AND v.cover_path != ''")
    if has_translation:
        conditions.append("v.title_zh IS NOT NULL AND v.title_zh != ''")
    if scrape_status:
        conditions.append("v.scrape_status = ?")
        params.append(scrape_status.upper())
    tag_conditions, tag_params = _tag_filter_conditions("v", tags)
    conditions.extend(tag_conditions)
    params.extend(tag_params)

    where = " AND ".join(conditions)
    representative_where = where.replace("v.", "v2.")

    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(DISTINCT v.code) AS total FROM videos v WHERE {where}",
            params,
        )
        total = int(cursor.fetchone()["total"])
        playback_join = playback_fields = ''
        order = _order_clause(sort)
        if sort in ('last_played', 'watched'):
            ensure_playback_schema(conn)
            playback_fields = ', p.last_played_at, p.watched_ms'
            playback_join = f"""LEFT JOIN (
                SELECT video_key, MAX(last_played_at) AS last_played_at, SUM(watched_ms) AS watched_ms
                FROM playback_sessions GROUP BY video_key
            ) p ON p.video_key=({VIDEO_KEY_SQL})"""
            order = ('p.last_played_at' if sort == 'last_played' else 'p.watched_ms') + ' DESC, v.id DESC'
        cursor.execute(
            f"""
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path, scrape_status, deleted{playback_fields}
            FROM videos v {playback_join}
            WHERE v.id IN (SELECT MIN(v2.id) FROM videos v2 WHERE {representative_where} GROUP BY v2.code)
            ORDER BY {order}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        )
        items = _rows_to_dicts(cursor.fetchall())
        return {"page": page, "limit": limit, "total": total, "items": items}
    except sqlite3.Error as e:
        logger.error(f"❌ 列表查询失败: {e}")
        return {"page": page, "limit": limit, "total": 0, "items": []}
    finally:
        if conn:
            conn.close()


def get_video_by_code(code: str) -> Optional[Dict[str, Any]]:
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return None

        cursor.execute(
            """
            SELECT id, code, part, title_jp, title_zh, title_zh_short, release_date, duration, maker, publisher,
                   series, score, cover_path, original_file_path, scrape_status,
                   file_size, file_mtime, file_birthtime, deleted
            FROM videos
            WHERE code = ?
            ORDER BY CAST(SUBSTR(COALESCE(part, '0'), 5) AS INTEGER) ASC, id ASC
            """,
            (resolved_code,),
        )
        video_rows = cursor.fetchall()
        if not video_rows:
            return None

        base = dict(video_rows[0])
        if len(video_rows) > 1:
            base['parts'] = [
                {
                    'id': dict(r)['id'],
                    'part': dict(r)['part'],
                    'original_file_path': dict(r)['original_file_path'],
                    'file_size': dict(r)['file_size'],
                    'file_mtime': dict(r)['file_mtime'],
                    'file_birthtime': dict(r)['file_birthtime'],
                }
                for r in video_rows
            ]
            sizes = [r['file_size'] for r in video_rows if r['file_size'] is not None]
            base['total_file_size'] = sum(sizes) if sizes else None
        else:
            base['total_file_size'] = base.get('file_size')

        cursor.execute(
            """
            SELECT DISTINCT a.id, a.name, a.name_zh, a.avatar_path
            FROM actors a
            JOIN video_actor_link val ON val.actor_id = a.id
            JOIN videos v ON v.id = val.video_id
            WHERE v.code = ? AND a.is_ignored = 0
            ORDER BY a.name
            """,
            (resolved_code,),
        )
        actors = _rows_to_dicts(cursor.fetchall())

        cursor.execute(
            """
            SELECT DISTINCT t.id, t.name
            FROM tags t
            JOIN video_tag_link vtl ON vtl.tag_id = t.id
            JOIN videos v ON v.id = vtl.video_id
            WHERE v.code = ?
            ORDER BY t.name
            """,
            (resolved_code,),
        )
        tags = _rows_to_dicts(cursor.fetchall())

        base["actors"] = actors
        base["tags"] = tags

        if base.get('series') and _has_series_catalog(conn):
            series = conn.execute("""SELECT j.id AS series_jp_id, c.id_final AS series_final_id,
                COALESCE(f.name,c.name) AS series_zh, c.status AS series_translation_status
                FROM videos v JOIN series_jp j ON j.id=v.series_jp_id
                LEFT JOIN series_cn c ON c.id_jp=j.id
                LEFT JOIN series_final f ON f.id=c.id_final WHERE v.id=?""", (base['id'],)).fetchone()
            if series:
                base.update(dict(series))

        return base
    except sqlite3.Error as e:
        logger.error(f"❌ 查询影片详情失败 [{code}]: {e}")
        return None
    finally:
        if conn:
            conn.close()


def search_videos(query: str, page: int = 1, limit: int = 24) -> Dict[str, Any]:
    page = max(1, page)
    limit = max(1, min(limit, 200))
    offset = (page - 1) * limit
    like_q = f"%{query}%"

    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(DISTINCT code) AS total
            FROM videos
            WHERE deleted = 0 AND (code LIKE ? OR title_jp LIKE ? OR title_zh LIKE ? OR maker LIKE ?)
            """,
            (like_q, like_q, like_q, like_q),
        )
        total = int(cursor.fetchone()["total"])
        cursor.execute(
            """
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path, scrape_status, deleted
            FROM videos
            WHERE id IN (
                SELECT MIN(id) FROM videos
                WHERE code LIKE ? OR title_jp LIKE ? OR title_zh LIKE ? OR maker LIKE ?
                GROUP BY code
            )
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (like_q, like_q, like_q, like_q, limit, offset),
        )
        items = _rows_to_dicts(cursor.fetchall())
        return {"page": page, "limit": limit, "total": total, "items": items}
    except sqlite3.Error as e:
        logger.error(f"❌ 搜索失败 [{query}]: {e}")
        return {"page": page, "limit": limit, "total": 0, "items": []}
    finally:
        if conn:
            conn.close()


def get_actor_with_videos(actor_id: int) -> Optional[Dict[str, Any]]:
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, name, name_zh, avatar_path FROM actors WHERE id = ?",
            (actor_id,),
        )
        actor = cursor.fetchone()
        if not actor:
            return None

        cursor.execute(
            """
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted, v.file_mtime
            FROM videos v
            WHERE v.id IN (
                SELECT MIN(v2.id)
                FROM videos v2
                JOIN video_actor_link val2 ON val2.video_id = v2.id
                WHERE val2.actor_id = ?
                GROUP BY v2.code
            )
            ORDER BY COALESCE(v.release_date, '') DESC, v.id DESC
            """,
            (actor_id,),
        )
        videos = _rows_to_dicts(cursor.fetchall())
        # Attach tags for each video (for frontend filtering)
        for v in videos:
            cursor.execute(
                """
                SELECT t.id, t.name FROM tags t
                JOIN video_tag_link vtl ON vtl.tag_id = t.id
                JOIN videos vv ON vv.id = vtl.video_id
                WHERE vv.code = ? ORDER BY t.name
                """,
                (v['code'],),
            )
            v['tags'] = _rows_to_dicts(cursor.fetchall())
        return {"actor": dict(actor), "videos": videos}
    except sqlite3.Error as e:
        logger.error(f"❌ 查询演员失败 [{actor_id}]: {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_tag_with_videos(tag_id: int) -> Optional[Dict[str, Any]]:
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM tags WHERE id = ?", (tag_id,))
        tag = cursor.fetchone()
        if not tag:
            return None

        cursor.execute(
            """
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted, v.file_mtime
            FROM videos v
            WHERE v.id IN (
                SELECT MIN(v2.id)
                FROM videos v2
                JOIN video_tag_link vtl2 ON vtl2.video_id = v2.id
                WHERE vtl2.tag_id = ?
                GROUP BY v2.code
            )
            ORDER BY COALESCE(v.release_date, '') DESC, v.id DESC
            """,
            (tag_id,),
        )
        return {"tag": dict(tag), "videos": _rows_to_dicts(cursor.fetchall())}
    except sqlite3.Error as e:
        logger.error(f"❌ 查询标签失败 [{tag_id}]: {e}")
        return None
    finally:
        if conn:
            conn.close()


def search_actors(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Search actors by name or name_zh, return id/name/name_zh/avatar_path."""
    like_q = f"%{_actor_name_key(q)}%"
    conn = None
    try:
        conn = _get_conn()
        conn.create_function('actor_name_key', 1, _actor_name_key)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, name, name_zh, avatar_path
            FROM actors
            WHERE is_ignored = 0 AND (actor_name_key(name) LIKE ? OR actor_name_key(name_zh) LIKE ?)
            ORDER BY CASE WHEN name_zh IS NOT NULL AND name_zh != '' THEN 0 ELSE 1 END, name_zh, name
            LIMIT ?
            """,
            (like_q, like_q, limit),
        )
        return _rows_to_dicts(cursor.fetchall())
    except sqlite3.Error as e:
        logger.error(f"❌ search_actors 失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


def _has_series_catalog(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='series_jp'").fetchone() is not None


def search_series(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Find source entries by raw, translated or reviewed name."""
    with closing(_get_conn()) as conn:
        like = f'%{q}%'
        if not _has_series_catalog(conn):
            return _rows_to_dicts(conn.execute("""SELECT DISTINCT series AS name FROM videos
                WHERE deleted=0 AND series LIKE ? ORDER BY series LIMIT ?""", (like, limit)).fetchall())
        return _rows_to_dicts(conn.execute("""SELECT DISTINCT j.raw_name AS name,
            COALESCE(f.name,c.name) AS name_zh FROM series_jp j
            JOIN videos v ON v.series_jp_id=j.id AND v.deleted=0
            LEFT JOIN series_cn c ON c.id_jp=j.id LEFT JOIN series_final f ON f.id=c.id_final
            WHERE j.raw_name LIKE ? OR c.name LIKE ? OR f.name LIKE ?
            ORDER BY name LIMIT ?""", (like, like, like, limit)).fetchall())


def patch_video_relations(code: str, actor_names: Optional[List[str]] = None, tag_names: Optional[List[str]] = None) -> bool:
    """Replace actor and/or tag links for all rows sharing the given code."""
    if actor_names is None and tag_names is None:
        return True
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM videos WHERE code = ?", (code,))
        video_ids = [r[0] for r in cursor.fetchall()]
        if not video_ids:
            return False
        ph = ','.join('?' * len(video_ids))
        if actor_names is not None:
            cursor.execute(f"DELETE FROM video_actor_link WHERE video_id IN ({ph})", video_ids)
            for entry in actor_names:
                if isinstance(entry, dict):
                    name = (entry.get('name') or '').strip()
                    name_zh = (entry.get('name_zh') or '').strip()
                else:
                    name = entry.strip()
                    name_zh = ''
                if not name:
                    continue
                actor_id, _, _ = _get_or_create_actor(cursor, name, name_zh)
                for vid in video_ids:
                    cursor.execute("INSERT OR IGNORE INTO video_actor_link (video_id, actor_id) VALUES (?, ?)", (vid, actor_id))
        if tag_names is not None:
            cursor.execute(f"DELETE FROM video_tag_link WHERE video_id IN ({ph})", video_ids)
            for name in tag_names:
                name = _to_simplified(name.strip())
                if not name:
                    continue
                cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (name,))
                cursor.execute("SELECT id FROM tags WHERE name = ?", (name,))
                row = cursor.fetchone()
                if row:
                    for vid in video_ids:
                        cursor.execute("INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)", (vid, row[0]))
        conn.commit()
        logger.info(f"✏️ patch_video_relations [{code}]: actors={actor_names is not None}, tags={tag_names is not None}")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ patch_video_relations 失败 [{code}]: {e}")
        return False
    finally:
        if conn:
            conn.close()


def add_existing_tags_to_video(code: str, tag_names: List[str]) -> bool:
    """Add existing tags to every active row for a code without replacing other tags."""
    if not tag_names:
        return True
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return False
        cursor.execute("SELECT id FROM videos WHERE code = ? AND deleted = 0", (resolved_code,))
        video_ids = [row["id"] for row in cursor.fetchall()]
        if not video_ids:
            return False
        placeholders = ",".join("?" * len(tag_names))
        cursor.execute(
            f"SELECT id FROM tags WHERE name IN ({placeholders})",
            tag_names,
        )
        tag_ids = [row["id"] for row in cursor.fetchall()]
        for video_id in video_ids:
            cursor.executemany(
                "INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)",
                [(video_id, tag_id) for tag_id in tag_ids],
            )
        conn.commit()
        return len(tag_ids) == len(set(tag_names))
    except sqlite3.Error as e:
        logger.error(f"❌ add_existing_tags_to_video 失败 [{code}]: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_stats() -> Dict[str, Any]:
    """Return aggregate stats for the analytics dashboard."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()

        # Totals (distinct codes only)
        cursor.execute("SELECT COUNT(DISTINCT code) FROM videos WHERE deleted=0")
        total_videos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT code) FROM videos WHERE deleted=1")
        deleted_videos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM actors")
        total_actors = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM tags")
        total_tags = cursor.fetchone()[0]
        total_series = len(_series_entries(conn))

        # Scrape status
        cursor.execute("SELECT scrape_status, COUNT(*) as cnt FROM videos GROUP BY scrape_status")
        scrape_status = {r['scrape_status']: r['cnt'] for r in cursor.fetchall()}

        # Translation coverage (distinct codes)
        cursor.execute("SELECT COUNT(DISTINCT code) FROM videos WHERE deleted=0 AND title_zh IS NOT NULL AND title_zh != ''")
        translated = cursor.fetchone()[0]

        # Cover coverage (distinct codes)
        cursor.execute("SELECT COUNT(DISTINCT code) FROM videos WHERE deleted=0 AND cover_path IS NOT NULL AND cover_path != ''")
        has_cover = cursor.fetchone()[0]

        # By year (top 12)
        cursor.execute("""
            SELECT substr(release_date,1,4) as year, COUNT(*) as cnt
            FROM videos WHERE deleted=0 AND release_date IS NOT NULL AND release_date != ''
            AND substr(release_date,1,4) GLOB '[0-9][0-9][0-9][0-9]'
            GROUP BY year ORDER BY year DESC LIMIT 12
        """)
        by_year = [{'year': r['year'], 'count': r['cnt']} for r in cursor.fetchall()]

        # Score distribution (ordered high → low via numeric sort key)
        cursor.execute("""
            SELECT CASE
                WHEN score>=4.5 THEN '4.5+'
                WHEN score>=4.0 THEN '4.0-4.5'
                WHEN score>=3.5 THEN '3.5-4.0'
                WHEN score>=3.0 THEN '3.0-3.5'
                ELSE '<3.0'
            END as range,
            CASE
                WHEN score>=4.5 THEN 5
                WHEN score>=4.0 THEN 4
                WHEN score>=3.5 THEN 3
                WHEN score>=3.0 THEN 2
                ELSE 1
            END as sort_order,
            COUNT(*) as cnt
            FROM videos WHERE deleted=0 AND score > 0
            GROUP BY range ORDER BY sort_order DESC
        """)
        score_dist = [{'range': r['range'], 'count': r['cnt']} for r in cursor.fetchall()]

        # Top makers
        cursor.execute("""
            SELECT maker, COUNT(*) as cnt FROM videos
            WHERE deleted=0 AND maker IS NOT NULL AND maker != ''
            GROUP BY maker ORDER BY cnt DESC LIMIT 10
        """)
        top_makers = [{'name': r['maker'], 'count': r['cnt']} for r in cursor.fetchall()]

        # Top actors
        cursor.execute("""
            SELECT a.id, COALESCE(a.name_zh, a.name) as name, a.avatar_path, COUNT(DISTINCT v.code) as cnt
            FROM actors a
            JOIN video_actor_link val ON val.actor_id = a.id
            JOIN videos v ON v.id = val.video_id AND v.deleted = 0
            WHERE a.is_ignored = 0
            GROUP BY a.id ORDER BY cnt DESC LIMIT 10
        """)
        top_actors = [{'id': r['id'], 'name': r['name'], 'avatar_path': r['avatar_path'], 'count': r['cnt']} for r in cursor.fetchall()]

        # Top actors by disk usage. For multi-actor videos, the file size counts
        # toward each linked actor so the list answers "space represented by this actor".
        cursor.execute("""
            SELECT
                a.id,
                COALESCE(a.name_zh, a.name) as name,
                a.avatar_path,
                COUNT(DISTINCT v.code) as video_count,
                COUNT(v.id) as file_count,
                COALESCE(SUM(COALESCE(v.file_size, 0)), 0) as size_bytes
            FROM actors a
            JOIN video_actor_link val ON val.actor_id = a.id
            JOIN videos v ON v.id = val.video_id AND v.deleted = 0
            WHERE a.is_ignored = 0
            GROUP BY a.id
            HAVING size_bytes > 0
            ORDER BY size_bytes DESC
            LIMIT 50
        """)
        actor_disk_usage = [
            {
                'id': r['id'],
                'name': r['name'],
                'avatar_path': r['avatar_path'],
                'video_count': r['video_count'],
                'file_count': r['file_count'],
                'size_bytes': r['size_bytes'],
                'size_gb': round(r['size_bytes'] / (1024 ** 3), 1),
            }
            for r in cursor.fetchall()
        ]

        # Top tags
        cursor.execute("""
            SELECT t.id, t.name, COUNT(DISTINCT v.code) as cnt
            FROM tags t
            JOIN video_tag_link vtl ON vtl.tag_id = t.id
            JOIN videos v ON v.id = vtl.video_id AND v.deleted = 0
            GROUP BY t.id ORDER BY cnt DESC LIMIT 15
        """)
        top_tags = [{'id': r['id'], 'name': r['name'], 'count': r['cnt']} for r in cursor.fetchall()]

        return {
            'total_videos': total_videos,
            'deleted_videos': deleted_videos,
            'total_actors': total_actors,
            'total_tags': total_tags,
            'total_series': total_series,
            'scrape_status': scrape_status,
            'translated': translated,
            'has_cover': has_cover,
            'by_year': by_year,
            'score_dist': score_dist,
            'top_makers': top_makers,
            'top_actors': top_actors,
            'actor_disk_usage': actor_disk_usage,
            'top_tags': top_tags,
        }
    except sqlite3.Error as e:
        logger.error(f"get_stats failed: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_recommendations(count: int = 8, tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Return `count` diverse successful videos with covers (no VR tags).

    Uses a single SQL pass: assigns each qualifying video its lexicographically
    first non-VR tag, shuffles with RANDOM(), then picks one video per tag
    bucket via GROUP BY.  Fast even on large libraries.
    """
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()

        # Pre-compute the set of video IDs that have any VR tag — single scan.
        cursor.execute(
            """
            SELECT DISTINCT v.id
            FROM videos v
            JOIN video_tag_link vtl ON vtl.video_id = v.id
            JOIN tags t ON t.id = vtl.tag_id
            WHERE UPPER(t.name) LIKE '%VR%'
            """
        )
        vr_ids = {r[0] for r in cursor.fetchall()}

        # Candidates: one representative row per code, has cover.
        tag_conditions, tag_params = _tag_filter_conditions("videos", tags)
        tag_where = "".join(f"\n              AND {condition}" for condition in tag_conditions)
        cursor.execute(
            f"""
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path
            FROM videos
            WHERE deleted = 0
              AND scrape_status = 'SUCCESS'
              AND cover_path IS NOT NULL
              AND cover_path != ''
              {tag_where}
              AND id = (SELECT MIN(id) FROM videos v2 WHERE v2.code = videos.code)
            ORDER BY RANDOM()
            LIMIT 500
            """,
            tag_params,
        )
        candidates = [dict(r) for r in cursor.fetchall() if r['id'] not in vr_ids]
        if not candidates:
            return []

        # For each candidate get its first non-VR tag (batch fetch by code list).
        codes = [c['code'] for c in candidates]
        placeholders = ','.join('?' * len(codes))
        cursor.execute(
            f"""
            SELECT vv.code, MIN(t.name) AS tag
            FROM videos vv
            JOIN video_tag_link vtl ON vtl.video_id = vv.id
            JOIN tags t ON t.id = vtl.tag_id
            WHERE vv.code IN ({placeholders})
              AND UPPER(t.name) NOT LIKE '%VR%'
            GROUP BY vv.code
            """,
            codes,
        )
        code_tag = {r['code']: r['tag'] for r in cursor.fetchall()}

        # Pick one video per tag bucket.
        import random as _random
        _random.shuffle(candidates)
        seen_tags: set = set()
        seen_codes: set = set()
        picked: list = []
        leftover: list = []
        for c in candidates:
            if c['code'] in seen_codes:
                continue
            seen_codes.add(c['code'])
            tag = code_tag.get(c['code'], '__none__')
            if tag not in seen_tags:
                seen_tags.add(tag)
                picked.append(c)
                if len(picked) == count:
                    break
            else:
                leftover.append(c)

        if len(picked) < count:
            picked.extend(leftover[:count - len(picked)])

        _random.shuffle(picked)
        result = picked[:count]
        # Drop the internal 'id' field before returning
        for r in result:
            r.pop('id', None)
        return result

    except sqlite3.Error as e:
        logger.error(f"get_recommendations failed: {e}")
        return []
    finally:
        if conn:
            conn.close()


def list_all_tags_with_count() -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT t.id, t.name, COUNT(DISTINCT v.code) AS count
            FROM tags t
            JOIN video_tag_link vtl ON vtl.tag_id = t.id
            JOIN videos v ON v.id = vtl.video_id
            WHERE v.deleted = 0
            GROUP BY t.id, t.name
            ORDER BY count DESC, t.name
            """
        )
        return _rows_to_dicts(cursor.fetchall())
    except sqlite3.Error as e:
        logger.error(f"❌ list_all_tags_with_count 失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


def list_all_actors_with_count() -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.id, a.name, a.name_zh, a.avatar_path, COUNT(DISTINCT v.code) AS count
            FROM actors a
            JOIN video_actor_link val ON val.actor_id = a.id
            JOIN videos v ON v.id = val.video_id
            WHERE a.is_ignored = 0 AND v.deleted = 0
            GROUP BY a.id
            ORDER BY count DESC, a.name
            """
        )
        actors = _rows_to_dicts(cursor.fetchall())
        for actor in actors:
            names = {name for name in (actor['name'], actor['name_zh']) if name}
            variants = {variant for name in names for variant in (_actor_name_key(name), convert(name, 'zh-hant'))}
            actor['search_names'] = sorted(variants - names)
        return actors
    except sqlite3.Error as e:
        logger.error(f"❌ list_all_actors_with_count 失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


def _series_entries(conn):
    if not _has_series_catalog(conn):
        return _rows_to_dicts(conn.execute("""SELECT series, series AS raw_name, COUNT(DISTINCT code) AS count
            FROM videos WHERE deleted=0 AND trim(COALESCE(series,''))!=''
            GROUP BY series ORDER BY count DESC, series""").fetchall())
    return _rows_to_dicts(conn.execute("""SELECT f.id AS final_id,
        CASE WHEN f.id IS NULL THEN j.id END AS jp_id,
        COALESCE(f.name,MAX(c.name),MAX(v.series)) AS series,
        CASE WHEN f.id IS NULL THEN MAX(v.series) END AS raw_name,
        CASE WHEN f.id IS NULL THEN MAX(c.status) END AS translation_status,
        COUNT(DISTINCT v.code) AS count
        FROM videos v LEFT JOIN series_jp j ON j.id=v.series_jp_id
        LEFT JOIN series_cn c ON c.id_jp=j.id LEFT JOIN series_final f ON f.id=c.id_final
        WHERE v.deleted=0 AND trim(COALESCE(v.series,''))!=''
        GROUP BY f.id, CASE WHEN f.id IS NULL THEN j.id END,
            CASE WHEN j.id IS NULL THEN v.series END
        ORDER BY count DESC, series""").fetchall())


def list_all_series_with_count() -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        return _series_entries(conn)
    finally:
        conn.close()


def get_videos_by_series(series_name=None, jp_id=None, final_id=None, page=1, limit=24, sort='date'):
    page = max(1, page)
    limit = max(1, min(limit, 200))
    conn = _get_conn()
    try:
        if final_id:
            condition = 'v.series_jp_id IN (SELECT id_jp FROM series_cn WHERE id_final=?)'
            key = final_id
            label = conn.execute('SELECT name FROM series_final WHERE id=?', (key,)).fetchone()
        elif jp_id:
            condition, key = 'v.series_jp_id=?', jp_id
            label = conn.execute("""SELECT COALESCE(c.name,j.raw_name) FROM series_jp j
                LEFT JOIN series_cn c ON c.id_jp=j.id WHERE j.id=?""", (key,)).fetchone()
        else:
            condition, key, label = 'v.series=?', series_name, None
        matches = f'SELECT MIN(v.id) FROM videos v WHERE v.deleted=0 AND {condition} GROUP BY v.code'
        total = conn.execute(f'SELECT COUNT(*) FROM ({matches})', (key,)).fetchone()[0]
        rows = conn.execute(f"""SELECT id,code,title_jp,title_zh,release_date,score,cover_path,deleted
            FROM videos WHERE id IN ({matches}) ORDER BY {_order_clause(sort)} LIMIT ? OFFSET ?""",
                            (key, limit, (page - 1) * limit)).fetchall()
        return dict(page=page, limit=limit, total=total, items=_rows_to_dicts(rows),
                    name=label[0] if label else series_name)
    finally:
        conn.close()


def list_all_prefixes_with_count() -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
              UPPER(CASE WHEN INSTR(code, '-') > 0
                         THEN SUBSTR(code, 1, INSTR(code, '-') - 1)
                         ELSE code END) AS prefix,
              COUNT(DISTINCT code) AS count
            FROM videos
            WHERE deleted = 0
            GROUP BY prefix
            ORDER BY count DESC, prefix
            """
        )
        return _rows_to_dicts(cursor.fetchall())
    except sqlite3.Error as e:
        logger.error(f"❌ list_all_prefixes_with_count 失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_videos_by_prefix(prefix: str, page: int = 1, limit: int = 24, sort: str = 'date') -> Dict[str, Any]:
    page = max(1, page)
    limit = max(1, min(limit, 200))
    offset = (page - 1) * limit
    prefix_upper = prefix.upper()
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        prefix_expr = """
            UPPER(CASE WHEN INSTR(code, '-') > 0
                       THEN SUBSTR(code, 1, INSTR(code, '-') - 1)
                       ELSE code END)
        """
        cursor.execute(
            f"SELECT COUNT(DISTINCT code) AS total FROM videos WHERE deleted = 0 AND {prefix_expr} = ?",
            (prefix_upper,),
        )
        total = int(cursor.fetchone()["total"])
        cursor.execute(
            f"""
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path, deleted
            FROM videos
            WHERE {prefix_expr} = ?
              AND id IN (SELECT MIN(id) FROM videos GROUP BY code)
            ORDER BY {_order_clause(sort)}
            LIMIT ? OFFSET ?
            """,
            (prefix_upper, limit, offset),
        )
        return {"page": page, "limit": limit, "total": total, "items": _rows_to_dicts(cursor.fetchall())}
    except sqlite3.Error as e:
        logger.error(f"❌ get_videos_by_prefix 失败 [{prefix}]: {e}")
        return {"page": page, "limit": limit, "total": 0, "items": []}
    finally:
        if conn:
            conn.close()


def delete_video(code: str) -> Dict[str, Any]:
    """Set deleted=1 on all rows for code and delete physical files. Returns deleted file paths."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT id, original_file_path FROM videos WHERE code = ?", (code,))
        rows = cursor.fetchall()
        if not rows:
            return {"ok": False, "error": "not found"}
        deleted_files = []
        for row in rows:
            path = dict(row)["original_file_path"]
            if path:
                try:
                    Path(path).unlink(missing_ok=True)
                    deleted_files.append(path)
                except Exception as e:
                    logger.warning(f"⚠️ 删除文件失败 [{path}]: {e}")
        cursor.execute("UPDATE videos SET deleted = 1, updated_at = CURRENT_TIMESTAMP WHERE code = ?", (code,))
        conn.commit()
        logger.info(f"🗑️ delete_video [{code}]: deleted {len(deleted_files)} files")
        return {"ok": True, "deleted_files": deleted_files}
    except sqlite3.Error as e:
        logger.error(f"❌ delete_video 失败 [{code}]: {e}")
        return {"ok": False, "error": str(e)}
    finally:
        if conn:
            conn.close()


def get_video_file_path(code: str, part: Optional[str] = None) -> Optional[str]:
    """Return original_file_path for given code (and optionally part)."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return None

        if part:
            cursor.execute(
                "SELECT original_file_path FROM videos WHERE code = ? AND part = ? ORDER BY id ASC LIMIT 1",
                (resolved_code, part),
            )
        else:
            cursor.execute(
                "SELECT original_file_path FROM videos WHERE code = ? ORDER BY id ASC LIMIT 1",
                (resolved_code,),
            )
        row = cursor.fetchone()
        return row["original_file_path"] if row else None
    except sqlite3.Error as e:
        logger.error(f"❌ get_video_file_path 失败 [{code}]: {e}")
        return None
    finally:
        if conn:
            conn.close()


def patch_video_file_path(code: str, original_file_path: str, part: Optional[str] = None) -> Dict[str, Any]:
    """Update original_file_path for exactly one video row."""
    if not original_file_path or not str(original_file_path).strip():
        return {"ok": False, "error": "missing original_file_path"}

    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return {"ok": False, "error": "not found"}

        if part:
            cursor.execute(
                "SELECT id, part FROM videos WHERE code = ? AND part = ? ORDER BY id ASC",
                (resolved_code, part),
            )
        else:
            cursor.execute("SELECT id, part FROM videos WHERE code = ? ORDER BY id ASC", (resolved_code,))

        rows = cursor.fetchall()
        if not rows:
            return {"ok": False, "error": "part not found" if part else "not found"}
        if len(rows) > 1 and not part:
            return {"ok": False, "error": "multiple rows match; specify part"}

        video_id = rows[0]["id"]
        cursor.execute(
            "UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (str(original_file_path).strip(), video_id),
        )
        conn.commit()
        return {"ok": True, "id": video_id, "code": resolved_code, "part": rows[0]["part"], "original_file_path": str(original_file_path).strip()}
    except sqlite3.IntegrityError as e:
        return {"ok": False, "error": str(e)}
    except sqlite3.Error as e:
        logger.error(f"❌ patch_video_file_path 失败 [{code}]: {e}")
        return {"ok": False, "error": str(e)}
    finally:
        if conn:
            conn.close()


def replace_video_file(
    code: str,
    original_part: Optional[str],
    dest_path: str,
    subtitle_zh: bool,
    uncensored_leaked: bool,
    uncensored_native: bool,
    file_meta: Optional[Dict[str, Any]] = None,
    uncensored_crack_tag: Optional[str] = None,
    subtitle_classification: Optional[str] = None,
    *, before_commit=None, expected_path=None, new_part=None,
) -> Dict[str, Any]:
    """Update file path and sync 中文字幕/无码破解分类/原装无码 tags after a file replacement.

    file_meta, when provided, carries the new file's stat (file_size/file_mtime/
    file_birthtime) so those columns stay in sync with the replaced file.
    """
    crack_tag = uncensored_crack_tag if uncensored_crack_tag in _UNCENSORED_CRACK_TAGS else _uncensored_crack_tag(
        (file_meta or {}).get('file_birthtime') or (file_meta or {}).get('file_mtime')
    )
    subtitle_tag = subtitle_classification or ('中文字幕' if subtitle_zh else None)
    tag_desired = {crack_tag: uncensored_leaked, '原装无码': uncensored_native}
    if subtitle_tag:
        tag_desired[subtitle_tag] = True
    conn = None
    try:
        conn = _get_conn()
        if before_commit is not None:
            conn.execute("BEGIN IMMEDIATE")
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return {'ok': False, 'error': 'not found'}

        if original_part:
            cursor.execute(
                'SELECT id, code, original_file_path FROM videos WHERE code = ? AND part = ? ORDER BY id ASC LIMIT 1',
                (resolved_code, original_part),
            )
        else:
            cursor.execute(
                'SELECT id, code, original_file_path FROM videos WHERE code = ? AND (part IS NULL OR part = "") ORDER BY id ASC LIMIT 1',
                (resolved_code,),
            )
        row = cursor.fetchone()
        if not row:
            return {'ok': False, 'error': 'part not found' if original_part else 'not found'}
        if expected_path is not None and row['original_file_path'] != expected_path:
            raise FileExistsError('原文件路径已改变，请重新选择')
        video_id = row['id']
        if new_part is not None:
            conflict = cursor.execute('SELECT 1 FROM videos WHERE code=? AND part=? AND id<>?',
                                      (resolved_code, new_part, video_id)).fetchone()
            if conflict:
                raise FileExistsError('目标分集已存在')
            cursor.execute('UPDATE videos SET part=? WHERE id=?', (new_part, video_id))

        # Keep the code column's version suffix (-C/-U/-UC) in sync with the new
        # file. Otherwise a -C record replaced by a -UC file keeps a stale -C code.
        has_c = subtitle_zh
        has_u = uncensored_leaked or uncensored_native
        suffix = '-UC' if (has_c and has_u) else '-C' if has_c else '-U' if has_u else ''
        new_code = re.sub(r'(?i)-(?:uc|u|c)$', '', row['code']) + suffix

        cursor.execute('SELECT id FROM videos WHERE code = ?', (resolved_code,))
        all_ids = [r['id'] for r in cursor.fetchall()]
        ph = ','.join('?' * len(all_ids))

        cursor.execute(
            f'SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id = t.id WHERE vtl.video_id IN ({ph})',
            all_ids,
        )
        current_tags = {r['name'] for r in cursor.fetchall()}

        cursor.execute(
            f"DELETE FROM video_tag_link WHERE video_id IN ({ph}) "
            "AND tag_id IN (SELECT id FROM tags WHERE name IN ('机翻中文', '中文字幕'))",
            all_ids,
        )
        current_tags.difference_update({'机翻中文', '中文字幕'})

        cracked_names = _LEGACY_UNCENSORED_CRACK_TAGS + _UNCENSORED_CRACK_TAGS
        cursor.execute(
            f'DELETE FROM video_tag_link WHERE video_id IN ({ph}) '
            f'AND tag_id IN (SELECT id FROM tags WHERE name IN ({",".join("?" * len(cracked_names))}))',
            all_ids + list(cracked_names),
        )
        current_tags.difference_update(cracked_names)

        for tag_name, should_have in tag_desired.items():
            if should_have and tag_name not in current_tags:
                cursor.execute('INSERT OR IGNORE INTO tags (name) VALUES (?)', (tag_name,))
                cursor.execute('SELECT id FROM tags WHERE name = ?', (tag_name,))
                tag_row = cursor.fetchone()
                if tag_row:
                    for vid in all_ids:
                        cursor.execute(
                            'INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)',
                            (vid, tag_row['id']),
                        )
            elif not should_have and tag_name in current_tags:
                cursor.execute('SELECT id FROM tags WHERE name = ?', (tag_name,))
                tag_row = cursor.fetchone()
                if tag_row:
                    cursor.execute(
                        f'DELETE FROM video_tag_link WHERE video_id IN ({ph}) AND tag_id = ?',
                        all_ids + [tag_row['id']],
                    )

        # Path/code/stats update last, so the code change above doesn't disturb the
        # resolved_code-based sibling lookup used for tag syncing.
        if file_meta:
            cursor.execute(
                'UPDATE videos SET original_file_path = ?, code = ?, file_size = ?, file_mtime = ?, '
                'file_birthtime = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (dest_path, new_code, file_meta.get('file_size'), file_meta.get('file_mtime'),
                 file_meta.get('file_birthtime'), video_id),
            )
        else:
            cursor.execute(
                'UPDATE videos SET original_file_path = ?, code = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (dest_path, new_code, video_id),
            )

        if before_commit is not None:
            before_commit()
        conn.commit()
        logger.info(f'🔄 replace_video_file [{code}]: {dest_path}')
        return {'ok': True, 'original_file_path': dest_path, 'new_code': new_code}
    except sqlite3.Error as e:
        logger.error(f'❌ replace_video_file 失败 [{code}]: {e}')
        return {'ok': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def replace_multipart_with_single_file(
    code: str,
    dest_path: str,
    subtitle_zh: bool,
    uncensored_leaked: bool,
    uncensored_native: bool,
    file_meta: Optional[Dict[str, Any]] = None,
    keep_part: Optional[str] = None,
    uncensored_crack_tag: Optional[str] = None,
    subtitle_classification: Optional[str] = None,
    *, before_commit=None, expected_path=None,
) -> Dict[str, Any]:
    """Collapse all rows for a code into one non-part row pointing at dest_path."""
    crack_tag = uncensored_crack_tag if uncensored_crack_tag in _UNCENSORED_CRACK_TAGS else _uncensored_crack_tag(
        (file_meta or {}).get('file_birthtime') or (file_meta or {}).get('file_mtime')
    )
    subtitle_tag = subtitle_classification or ('中文字幕' if subtitle_zh else None)
    tag_desired = {crack_tag: uncensored_leaked, '原装无码': uncensored_native}
    if subtitle_tag:
        tag_desired[subtitle_tag] = True
    conn = None
    try:
        conn = _get_conn()
        if before_commit is not None:
            conn.execute("BEGIN IMMEDIATE")
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return {'ok': False, 'error': 'not found'}

        cursor.execute(
            'SELECT id, code, part, original_file_path FROM videos WHERE code = ? ORDER BY id ASC',
            (resolved_code,),
        )
        rows = cursor.fetchall()
        if not rows:
            return {'ok': False, 'error': 'not found'}

        keep_row = None
        if keep_part:
            keep_row = next((r for r in rows if (r['part'] or '') == keep_part), None)
            if keep_row is None:
                return {'ok': False, 'error': 'part not found'}
        else:
            keep_row = rows[0]

        if expected_path is not None and keep_row['original_file_path'] != expected_path:
            raise FileExistsError('原文件路径已改变，请重新选择')
        keep_id = keep_row['id']
        all_ids = [r['id'] for r in rows]
        remove_ids = [vid for vid in all_ids if vid != keep_id]
        old_paths = [r['original_file_path'] for r in rows if r['original_file_path']]
        ph = ','.join('?' * len(all_ids))

        has_c = subtitle_zh
        has_u = uncensored_leaked or uncensored_native
        suffix = '-UC' if (has_c and has_u) else '-C' if has_c else '-U' if has_u else ''
        new_code = re.sub(r'(?i)-(?:uc|u|c)$', '', keep_row['code']) + suffix

        cursor.execute(
            f'SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id = t.id WHERE vtl.video_id IN ({ph})',
            all_ids,
        )
        current_tags = {r['name'] for r in cursor.fetchall()}

        cursor.execute(
            "DELETE FROM video_tag_link WHERE video_id = ? "
            "AND tag_id IN (SELECT id FROM tags WHERE name IN ('机翻中文', '中文字幕'))",
            (keep_id,),
        )
        current_tags.difference_update({'机翻中文', '中文字幕'})

        cracked_names = _LEGACY_UNCENSORED_CRACK_TAGS + _UNCENSORED_CRACK_TAGS
        cursor.execute(
            'DELETE FROM video_tag_link WHERE video_id = ? '
            f'AND tag_id IN (SELECT id FROM tags WHERE name IN ({",".join("?" * len(cracked_names))}))',
            [keep_id] + list(cracked_names),
        )
        current_tags.difference_update(cracked_names)

        for tag_name, should_have in tag_desired.items():
            if should_have:
                cursor.execute('INSERT OR IGNORE INTO tags (name) VALUES (?)', (tag_name,))
                cursor.execute('SELECT id FROM tags WHERE name = ?', (tag_name,))
                tag_row = cursor.fetchone()
                if tag_row:
                    cursor.execute(
                        'INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)',
                        (keep_id, tag_row['id']),
                    )
            elif not should_have and tag_name in current_tags:
                cursor.execute('SELECT id FROM tags WHERE name = ?', (tag_name,))
                tag_row = cursor.fetchone()
                if tag_row:
                    cursor.execute(
                        'DELETE FROM video_tag_link WHERE video_id = ? AND tag_id = ?',
                        (keep_id, tag_row['id']),
                    )

        if remove_ids:
            remove_ph = ','.join('?' * len(remove_ids))
            cursor.execute(f'DELETE FROM video_actor_link WHERE video_id IN ({remove_ph})', remove_ids)
            cursor.execute(f'DELETE FROM video_tag_link WHERE video_id IN ({remove_ph})', remove_ids)
            cursor.execute(f'DELETE FROM videos WHERE id IN ({remove_ph})', remove_ids)

        if file_meta:
            cursor.execute(
                'UPDATE videos SET original_file_path = ?, code = ?, part = NULL, file_size = ?, '
                'file_mtime = ?, file_birthtime = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (dest_path, new_code, file_meta.get('file_size'), file_meta.get('file_mtime'),
                 file_meta.get('file_birthtime'), keep_id),
            )
        else:
            cursor.execute(
                'UPDATE videos SET original_file_path = ?, code = ?, part = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (dest_path, new_code, keep_id),
            )

        if before_commit is not None:
            before_commit()
        conn.commit()
        logger.info(f'🔄 replace_multipart_with_single_file [{code}]: {dest_path}')
        return {'ok': True, 'original_file_path': dest_path, 'new_code': new_code, 'old_paths': old_paths}
    except sqlite3.Error as e:
        logger.error(f'❌ replace_multipart_with_single_file 失败 [{code}]: {e}')
        return {'ok': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def rename_video_file(
    code: str,
    part: Optional[str],
    new_path: str,
    new_code: str,
) -> Dict[str, Any]:
    """Point a video record at a renamed file and keep sibling parts on one code.

    Physical file rename and filename assembly happen in the app layer. Only the
    selected row's path changes, while every part under the resolved code follows
    the code change.
    """
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return {'ok': False, 'error': 'not found'}

        if part:
            cursor.execute(
                'SELECT id FROM videos WHERE code = ? AND part = ? ORDER BY id ASC LIMIT 1',
                (resolved_code, part),
            )
        else:
            cursor.execute(
                'SELECT id FROM videos WHERE code = ? AND (part IS NULL OR part = "") ORDER BY id ASC LIMIT 1',
                (resolved_code,),
            )
        row = cursor.fetchone()
        if not row:
            return {'ok': False, 'error': 'part not found' if part else 'not found'}

        cursor.execute(
            'UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
            (new_path, row['id']),
        )
        cursor.execute(
            'UPDATE videos SET code = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?',
            (new_code, resolved_code),
        )
        conn.commit()
        logger.info(f'✏️  rename_video_file [{code} -> {new_code}]: {new_path}')
        return {'ok': True}
    except sqlite3.Error as e:
        logger.error(f'❌ rename_video_file 失败 [{code}]: {e}')
        return {'ok': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def patch_actor(actor_id: int, fields: Dict[str, Any]) -> Optional[int]:
    """Update an actor, returning the surviving ID if the new name matches another actor."""
    allowed = {key: _actor_name_key(value) if isinstance(value, str) else value
               for key, value in fields.items() if key in {'name', 'name_zh'}}
    if not allowed or ('name' in allowed and (not isinstance(allowed['name'], str) or not allowed['name'])):
        return None
    conn = None
    try:
        conn = _get_conn()
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.cursor()
        if not cursor.execute("SELECT 1 FROM actors WHERE id = ?", (actor_id,)).fetchone():
            return None
        if 'name' in allowed:
            conflict = _actor_by_name(cursor, fields['name'].strip(), exclude_id=actor_id, include_display=True)
            if conflict is not None:
                _merge_actor_rows(conn, actor_id, conflict[0])
                conn.commit()
                return conflict[0]
        set_clause = ', '.join(f"{key} = ?" for key in allowed)
        cursor.execute(f"UPDATE actors SET {set_clause} WHERE id = ?", [*allowed.values(), actor_id])
        conn.commit()
        return actor_id
    except sqlite3.Error as e:
        logger.error(f"❌ patch_actor 失败 [actor_id={actor_id}]: {e}")
        return None
    finally:
        if conn:
            conn.close()


def _merge_actor_rows(conn, source_id, target_id):
    source = conn.execute("SELECT avatar_path, name_zh FROM actors WHERE id = ?", (source_id,)).fetchone()
    target = conn.execute("SELECT avatar_path, name_zh FROM actors WHERE id = ?", (target_id,)).fetchone()
    if not source or not target or source_id == target_id:
        return False
    avatar = target[0] if target[0] not in (None, '', 'no_avatar') else source[0] or target[0]
    conn.execute("UPDATE actors SET avatar_path = ?, name_zh = ? WHERE id = ?",
                 (avatar, target[1] or source[1], target_id))
    conn.execute("""INSERT OR IGNORE INTO video_actor_link (video_id, actor_id)
        SELECT video_id, ? FROM video_actor_link WHERE actor_id = ?""", (target_id, source_id))
    conn.execute("DELETE FROM video_actor_link WHERE actor_id = ?", (source_id,))
    conn.execute("DELETE FROM actors WHERE id = ?", (source_id,))
    return True


def merge_actors(source_id: int, target_id: int) -> bool:
    """Move source links to the target and preserve missing profile fields."""
    conn = None
    try:
        conn = _get_conn()
        conn.execute("BEGIN IMMEDIATE")
        merged = _merge_actor_rows(conn, source_id, target_id)
        conn.commit()
        return merged
    except sqlite3.Error as e:
        logger.error(f"❌ merge_actors 失败: {e}")
        return False
    finally:
        if conn:
            conn.close()


def patch_actor_avatar(actor_id: int, avatar_path: str) -> bool:
    """Update avatar_path for a single actor."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("UPDATE actors SET avatar_path = ? WHERE id = ?", (avatar_path, actor_id))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"❌ patch_actor_avatar 失败 [actor_id={actor_id}]: {e}")
        return False
    finally:
        if conn:
            conn.close()


def patch_video_cover(code: str, cover_path: str) -> bool:
    """Update cover_path for all rows sharing the given code."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("UPDATE videos SET cover_path = ? WHERE code = ?", (cover_path, code))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"❌ patch_video_cover 失败 [{code}]: {e}")
        return False
    finally:
        if conn:
            conn.close()


def rename_video_code(old_code: str, new_code: str) -> bool:
    """Rename code for all rows sharing old_code. Fails if new_code already exists."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM videos WHERE code = ? LIMIT 1", (new_code,))
        if cursor.fetchone():
            logger.warning(f"rename_video_code: new_code '{new_code}' already exists")
            return False
        cursor.execute("UPDATE videos SET code = ? WHERE code = ?", (new_code, old_code))
        conn.commit()
        logger.info(f"✏️ rename_video_code: {old_code} → {new_code}")
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"❌ rename_video_code 失败: {e}")
        return False
    finally:
        if conn:
            conn.close()


def patch_video(code: str, fields: Dict[str, Any], *, series_store=None) -> bool:
    """Update simple scalar fields for all rows sharing the given code."""
    allowed = {k: v for k, v in fields.items() if k in PATCHABLE_FIELDS}
    if not allowed:
        return False
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        code = _resolve_video_code(cursor, code)
        if code is None:
            return False
        set_clause = ', '.join(f"{k} = ?" for k in allowed)
        values = list(allowed.values()) + [code]
        cursor.execute(
            f"UPDATE videos SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE code = ?",
            values,
        )
        changed = cursor.rowcount > 0
        if changed and series_store is not None:
            from core.series_catalog import attach
            row = conn.execute('SELECT series FROM videos WHERE code=? ORDER BY id LIMIT 1', (code,)).fetchone()
            attach(conn, series_store, code, row['series'])
        conn.commit()
        logger.info(f"✏️ patch_video [{code}]: {list(allowed.keys())}")
        return changed
    except sqlite3.Error as e:
        logger.error(f"❌ patch_video 失败 [{code}]: {e}")
        return False
    finally:
        if conn:
            conn.close()

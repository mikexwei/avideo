import sqlite3
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Any

# 为了解决模块导入路径问题，动态引入 config
import sys
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH

logger = logging.getLogger(__name__)


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]

def batch_insert_scanned_videos(scanned_results: list) -> tuple[int, int]:
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
            VALUES (?, ?, ?, 'PENDING', ?, ?, ?)
        """

        for item in valid_items:
            # 提取数据准备插入
            code = item['code']
            part = item.get('part')
            # 必须转成字符串存入数据库
            file_path_str = str(item['original_path'])

            cursor.execute(insert_sql, (
                code, part, file_path_str,
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

def get_pending_videos(limit: int = 5) -> List[Tuple[int, str]]:
    """
    从数据库中捞取指定数量的待刮削 (PENDING) 影片。
    返回格式: [(id, code), (id, code)...]
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 按创建时间先后顺序，每次捞取最老的几个 PENDING 任务
        sql = "SELECT id, code FROM videos WHERE scrape_status = 'PENDING' ORDER BY created_at ASC LIMIT ?"
        cursor.execute(sql, (limit,))
        results = cursor.fetchall()
        
        return results
    except sqlite3.Error as e:
        logger.error(f"❌ 捞取 PENDING 任务失败: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_video_metadata(video_id: int, status: str, data: Optional[Dict] = None):
    """
    架构师终极版：将刮削结果更新回数据库，并处理多对多关联 (演员、标签)
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 0. 获取与此 video_id 关联的番号 (code)，并找出所有拥有此 code 的视频 ID
        cursor.execute("SELECT code FROM videos WHERE id = ?", (video_id,))
        row = cursor.fetchone()
        if not row:
            logger.warning(f"⚠️ 未找到 ID 为 {video_id} 的视频记录。")
            return
        
        code = row[0]
        cursor.execute("SELECT id FROM videos WHERE code = ?", (code,))
        target_video_ids = [r[0] for r in cursor.fetchall()]
        
        if status == 'SUCCESS' and data:
            series_value = data.get('series', '')
            if series_value:
                try:
                    from core.translator.service import translate_series_if_japanese
                    series_value = translate_series_if_japanese(series_value)
                except Exception:
                    # 翻译异常不影响主流程，保留原始 series
                    pass

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
                data.get('maker', ''), data.get('publisher', ''), series_value,
                data.get('score', 0.0), data.get('cover_path', ''), code
            ))
            
            # 2. 处理演员 (Actors) 的多对多入库
            actors_str = data.get('actors', '')
            if actors_str:
                # 把逗号分隔的字符串拆成列表，并去掉两边空格
                actor_list = [a.strip() for a in actors_str.split(',') if a.strip()]
                for actor_name in actor_list:
                    # 尝试插入演员 (如果已存在则忽略)
                    cursor.execute("INSERT OR IGNORE INTO actors (name) VALUES (?)", (actor_name,))
                    # 查询该演员的 ID (无论他是刚插入的，还是以前就存在的)
                    cursor.execute("SELECT id FROM actors WHERE name = ?", (actor_name,))
                    actor_row = cursor.fetchone()
                    if actor_row:
                        actor_id = actor_row[0]
                        # 为所有具有相同 code 的 video_id 插入关联表
                        for t_v_id in target_video_ids:
                            cursor.execute("INSERT OR IGNORE INTO video_actor_link (video_id, actor_id) VALUES (?, ?)", (t_v_id, actor_id))

            # 3. 处理标签/类别 (Tags/Categories) 的多对多入库
            tags_str = data.get('categories', '')
            if tags_str:
                tag_list = [t.strip() for t in tags_str.split(',') if t.strip()]
                for tag_name in tag_list:
                    cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
                    cursor.execute("SELECT id FROM tags WHERE name = ?", (tag_name,))
                    tag_row = cursor.fetchone()
                    if tag_row:
                        tag_id = tag_row[0]
                        for t_v_id in target_video_ids:
                            cursor.execute("INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)", (t_v_id, tag_id))
            
        else:
            # 刮削失败，批量更新所有相同 code 的状态
            sql_fail = "UPDATE videos SET scrape_status = 'FAILED', updated_at = CURRENT_TIMESTAMP WHERE code = ?"
            cursor.execute(sql_fail, (code,))
            
        # 所有操作要么一起成功，要么一起失败 (事务保障)
        conn.commit()
        logger.debug(f"💾 数据库多表联动更新完毕: 番号 {code} (包含 {len(target_video_ids)} 个分集记录) -> 状态 {status}")
        
    except sqlite3.Error as e:
        logger.error(f"❌ 数据库多表更新失败 (ID: {video_id}): {e}")
        # 如果发生异常，数据库会自动回滚，防止产生脏数据
    finally:
        if conn:
            conn.close()

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

def update_actor_avatar(actor_id: int, avatar_path: Optional[str], actor_name: str = "", name_zh: Optional[str] = None):
    """更新演员头像路径，如果发现同图，则执行自动合并归一化"""
    conn = None
    try:
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
                conn.commit()
                logger.info(f"✅ 合并完毕: [{actor_name}] 的所有作品已归入 [{target_name}] 旗下。")
                return
                
        # 更新数据库
        if name_zh:
            cursor.execute("UPDATE actors SET avatar_path = ?, name_zh = ? WHERE id = ?", (path_to_save, name_zh, actor_id))
        else:
            cursor.execute("UPDATE actors SET avatar_path = ? WHERE id = ?", (path_to_save, actor_id))
        conn.commit()
        
        log_msg = f"💾 演员信息入库完毕 (ID: {actor_id}) -> 头像: {path_to_save}" + (f" | 译名: {name_zh}" if name_zh else "")
        logger.info(log_msg)
    except sqlite3.Error as e:
        logger.error(f"❌ 演员头像更新失败 (ID: {actor_id}): {e}")
    finally:
        if conn:
            conn.close()


def list_videos(page: int = 1, limit: int = 24) -> Dict[str, Any]:
    page = max(1, page)
    limit = max(1, min(limit, 200))
    offset = (page - 1) * limit

    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT code) AS total FROM videos")
        total = int(cursor.fetchone()["total"])
        cursor.execute(
            """
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path, scrape_status
            FROM videos
            WHERE id IN (SELECT MIN(id) FROM videos GROUP BY code)
            ORDER BY COALESCE(release_date, '') DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
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
        cursor.execute(
            """
            SELECT id, code, part, title_jp, title_zh, release_date, duration, maker, publisher,
                   series, score, cover_path, original_file_path, scrape_status,
                   file_size, file_mtime, file_birthtime
            FROM videos
            WHERE code = ?
            ORDER BY id ASC
            """,
            (code,),
        )
        video_rows = cursor.fetchall()
        if not video_rows:
            return None

        base = dict(video_rows[0])
        if len(video_rows) > 1:
            base['parts'] = [
                {'id': dict(r)['id'], 'part': dict(r)['part'], 'original_file_path': dict(r)['original_file_path']}
                for r in video_rows
            ]

        cursor.execute(
            """
            SELECT DISTINCT a.id, a.name, a.name_zh, a.avatar_path
            FROM actors a
            JOIN video_actor_link val ON val.actor_id = a.id
            JOIN videos v ON v.id = val.video_id
            WHERE v.code = ?
            ORDER BY a.name
            """,
            (code,),
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
            (code,),
        )
        tags = _rows_to_dicts(cursor.fetchall())

        base["actors"] = actors
        base["tags"] = tags
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
            WHERE code LIKE ? OR title_jp LIKE ? OR title_zh LIKE ? OR maker LIKE ?
            """,
            (like_q, like_q, like_q, like_q),
        )
        total = int(cursor.fetchone()["total"])
        cursor.execute(
            """
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path, scrape_status
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
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score
            FROM videos v
            JOIN video_actor_link val ON val.video_id = v.id
            WHERE val.actor_id = ?
            ORDER BY v.id DESC
            """,
            (actor_id,),
        )
        return {"actor": dict(actor), "videos": _rows_to_dicts(cursor.fetchall())}
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
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score
            FROM videos v
            JOIN video_tag_link vtl ON vtl.video_id = v.id
            WHERE vtl.tag_id = ?
            ORDER BY v.id DESC
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
    like_q = f"%{q}%"
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, name, name_zh, avatar_path
            FROM actors
            WHERE is_ignored = 0 AND (name LIKE ? OR name_zh LIKE ?)
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


def search_series(q: str, limit: int = 10) -> List[str]:
    """Return distinct series names containing q."""
    like_q = f"%{q}%"
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT series FROM videos
            WHERE series IS NOT NULL AND series != '' AND series LIKE ?
            ORDER BY series
            LIMIT ?
            """,
            (like_q, limit),
        )
        return [r[0] for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"❌ search_series 失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


PATCHABLE_FIELDS = {'title_jp', 'title_zh', 'release_date', 'duration', 'maker', 'publisher', 'series', 'score'}


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
            for name in actor_names:
                name = name.strip()
                if not name:
                    continue
                cursor.execute("INSERT OR IGNORE INTO actors (name) VALUES (?)", (name,))
                cursor.execute("SELECT id FROM actors WHERE name = ?", (name,))
                row = cursor.fetchone()
                if row:
                    for vid in video_ids:
                        cursor.execute("INSERT OR IGNORE INTO video_actor_link (video_id, actor_id) VALUES (?, ?)", (vid, row[0]))
        if tag_names is not None:
            cursor.execute(f"DELETE FROM video_tag_link WHERE video_id IN ({ph})", video_ids)
            for name in tag_names:
                name = name.strip()
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

def patch_video(code: str, fields: Dict[str, Any]) -> bool:
    """Update simple scalar fields for all rows sharing the given code."""
    allowed = {k: v for k, v in fields.items() if k in PATCHABLE_FIELDS}
    if not allowed:
        return False
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        set_clause = ', '.join(f"{k} = ?" for k in allowed)
        values = list(allowed.values()) + [code]
        cursor.execute(
            f"UPDATE videos SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE code = ?",
            values,
        )
        conn.commit()
        logger.info(f"✏️ patch_video [{code}]: {list(allowed.keys())}")
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"❌ patch_video 失败 [{code}]: {e}")
        return False
    finally:
        if conn:
            conn.close()

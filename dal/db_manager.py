import json
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

def _order_clause(sort: str) -> str:
    """Return the ORDER BY body for video queries."""
    if sort == 'score':
        return "COALESCE(score, 0) DESC, COALESCE(release_date, '') DESC, id DESC"
    return "COALESCE(release_date, '') DESC, id DESC"

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


def list_videos(page: int = 1, limit: int = 24, sort: str = 'date') -> Dict[str, Any]:
    page = max(1, page)
    limit = max(1, min(limit, 200))
    offset = (page - 1) * limit

    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT code) AS total FROM videos WHERE deleted = 0")
        total = int(cursor.fetchone()["total"])
        cursor.execute(
            f"""
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path, scrape_status, deleted
            FROM videos
            WHERE id IN (SELECT MIN(id) FROM videos GROUP BY code)
            ORDER BY {_order_clause(sort)}
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
                   file_size, file_mtime, file_birthtime, deleted
            FROM videos
            WHERE code = ?
            ORDER BY COALESCE(part, '') ASC, id ASC
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
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted
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
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted
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


def search_series(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Search series by Japanese name or Chinese cluster name.
    Returns list of {name, name_zh, cluster_id} dicts.
    """
    like_q = f"%{q}%"
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        # 1. clusters matching canonical_name_zh or canonical_name
        cursor.execute(
            """
            SELECT canonical_name AS name, canonical_name_zh AS name_zh, id AS cluster_id
            FROM series_clusters
            WHERE (canonical_name_zh LIKE ? OR canonical_name LIKE ?)
              AND canonical_name_zh IS NOT NULL AND canonical_name_zh != ''
            ORDER BY canonical_name_zh
            LIMIT ?
            """,
            (like_q, like_q, limit),
        )
        results = _rows_to_dicts(cursor.fetchall())
        cluster_names = {r['name'] for r in results}
        # 2. raw series from videos not already covered by a cluster result
        cursor.execute(
            """
            SELECT DISTINCT series AS name FROM videos
            WHERE series IS NOT NULL AND series != '' AND series LIKE ?
            ORDER BY series
            LIMIT ?
            """,
            (like_q, limit),
        )
        for r in cursor.fetchall():
            if r['name'] not in cluster_names:
                results.append({'name': r['name'], 'name_zh': None, 'cluster_id': None})
        return results[:limit]
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
            for entry in actor_names:
                if isinstance(entry, dict):
                    name = (entry.get('name') or '').strip()
                    name_zh = (entry.get('name_zh') or '').strip()
                else:
                    name = entry.strip()
                    name_zh = ''
                if not name:
                    continue
                # Find by name, else create
                cursor.execute("SELECT id FROM actors WHERE name = ?", (name,))
                row = cursor.fetchone()
                if not row:
                    cursor.execute("INSERT INTO actors (name, name_zh) VALUES (?, ?)", (name, name_zh or None))
                    cursor.execute("SELECT id FROM actors WHERE name = ?", (name,))
                    row = cursor.fetchone()
                elif name_zh:
                    cursor.execute("UPDATE actors SET name_zh = ? WHERE id = ? AND (name_zh IS NULL OR name_zh = '')", (name_zh, row[0]))
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
        return _rows_to_dicts(cursor.fetchall())
    except sqlite3.Error as e:
        logger.error(f"❌ list_all_actors_with_count 失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


def list_all_series_with_count() -> List[Dict[str, Any]]:
    """
    Return series list using series_clusters for aggregation.
    Clustered series show canonical_name_zh and sum counts across all variants.
    Unclustered series fall through with their raw name.
    """
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()

        # 1. Get per-series video counts from videos table
        cursor.execute(
            "SELECT series, COUNT(DISTINCT code) AS cnt FROM videos "
            "WHERE series IS NOT NULL AND series != '' AND deleted = 0 GROUP BY series"
        )
        series_counts: Dict[str, int] = {r["series"]: r["cnt"] for r in cursor.fetchall()}

        # 2. Load all clusters and build name → cluster_id mapping
        cursor.execute("SELECT id, canonical_name, canonical_name_zh, variations_json FROM series_clusters")
        clusters = _rows_to_dicts(cursor.fetchall())

        name_to_cluster_id: Dict[str, int] = {}
        for c in clusters:
            all_names = set([c["canonical_name"]] + json.loads(c["variations_json"] or "[]"))
            for name in all_names:
                if name and name not in name_to_cluster_id:
                    name_to_cluster_id[name] = c["id"]

        # 3. Aggregate counts by cluster; collect unclustered series
        cluster_counts: Dict[int, int] = {}
        unclustered: Dict[str, int] = {}
        for series, cnt in series_counts.items():
            cid = name_to_cluster_id.get(series)
            if cid:
                cluster_counts[cid] = cluster_counts.get(cid, 0) + cnt
            else:
                unclustered[series] = cnt

        # 4. Build result: clusters first (sorted by count), then unclustered
        cluster_map = {c["id"]: c for c in clusters}
        result: List[Dict[str, Any]] = []

        for cid, cnt in sorted(cluster_counts.items(), key=lambda x: -x[1]):
            c = cluster_map[cid]
            result.append({
                "id": cid,
                "series": c["canonical_name_zh"] or c["canonical_name"],
                "count": cnt,
                "type": "cluster",
            })
        for series, cnt in sorted(unclustered.items(), key=lambda x: -x[1]):
            result.append({"id": None, "series": series, "count": cnt, "type": "raw"})

        return result
    except Exception as e:
        logger.error(f"❌ list_all_series_with_count 失败: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_videos_by_series(series_name: Optional[str] = None, cluster_id: Optional[int] = None,
                         page: int = 1, limit: int = 24, sort: str = 'date') -> Dict[str, Any]:
    """
    Fetch videos for a series. If cluster_id is given, expands all cluster variants.
    Falls back to exact series_name match otherwise.
    """
    page = max(1, page)
    limit = max(1, min(limit, 200))
    offset = (page - 1) * limit
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()

        if cluster_id:
            cursor.execute(
                "SELECT canonical_name, variations_json FROM series_clusters WHERE id = ?",
                (cluster_id,),
            )
            row = cursor.fetchone()
            if row:
                row = dict(row)
                names = list(set([row["canonical_name"]] + json.loads(row["variations_json"] or "[]")))
            else:
                names = []
        else:
            names = [series_name] if series_name else []

        if not names:
            return {"page": page, "limit": limit, "total": 0, "items": []}

        ph = ",".join("?" * len(names))
        cursor.execute(f"SELECT COUNT(DISTINCT code) AS total FROM videos WHERE deleted = 0 AND series IN ({ph})", names)
        total = int(cursor.fetchone()["total"])
        cursor.execute(
            f"""
            SELECT id, code, title_jp, title_zh, release_date, score, cover_path, deleted
            FROM videos
            WHERE series IN ({ph})
              AND id IN (SELECT MIN(id) FROM videos GROUP BY code)
            ORDER BY {_order_clause(sort)}
            LIMIT ? OFFSET ?
            """,
            names + [limit, offset],
        )
        return {"page": page, "limit": limit, "total": total, "items": _rows_to_dicts(cursor.fetchall())}
    except Exception as e:
        logger.error(f"❌ get_videos_by_series 失败: {e}")
        return {"page": page, "limit": limit, "total": 0, "items": []}
    finally:
        if conn:
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
        if part:
            cursor.execute(
                "SELECT original_file_path FROM videos WHERE code = ? AND part = ? ORDER BY id ASC LIMIT 1",
                (code, part),
            )
        else:
            cursor.execute(
                "SELECT original_file_path FROM videos WHERE code = ? ORDER BY id ASC LIMIT 1",
                (code,),
            )
        row = cursor.fetchone()
        return row["original_file_path"] if row else None
    except sqlite3.Error as e:
        logger.error(f"❌ get_video_file_path 失败 [{code}]: {e}")
        return None
    finally:
        if conn:
            conn.close()


def patch_actor(actor_id: int, fields: Dict[str, Any]) -> bool:
    """Update editable scalar fields for an actor (name, name_zh)."""
    allowed = {k: v for k, v in fields.items() if k in {'name', 'name_zh'}}
    if not allowed:
        return False
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        set_clause = ', '.join(f"{k} = ?" for k in allowed)
        cursor.execute(f"UPDATE actors SET {set_clause} WHERE id = ?", list(allowed.values()) + [actor_id])
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"❌ patch_actor 失败 [actor_id={actor_id}]: {e}")
        return False
    finally:
        if conn:
            conn.close()


def merge_actors(source_id: int, target_id: int) -> bool:
    """Move all video links from source actor to target actor, then delete source."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        # Transfer links that don't already exist on target
        cursor.execute(
            """
            INSERT OR IGNORE INTO video_actor_link (video_id, actor_id)
            SELECT video_id, ? FROM video_actor_link WHERE actor_id = ?
            """,
            (target_id, source_id),
        )
        cursor.execute("DELETE FROM video_actor_link WHERE actor_id = ?", (source_id,))
        cursor.execute("DELETE FROM actors WHERE id = ?", (source_id,))
        conn.commit()
        logger.info(f"🔀 merge_actors: source={source_id} → target={target_id}")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ merge_actors 失败: {e}")
        return False
    finally:
        if conn:
            conn.close()


def find_actor_by_name(name: str, exclude_id: int) -> Optional[Dict[str, Any]]:
    """Return actor with given name (or name_zh) excluding exclude_id, or None."""
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, name, name_zh FROM actors WHERE (name = ? OR name_zh = ?) AND id != ?",
            (name, name, exclude_id),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {'id': row[0], 'name': row[1], 'name_zh': row[2]}
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


def assign_series_cluster(code: str, cluster_id: int) -> bool:
    """Assign a video's series to an existing cluster.
    - Adds the video's current series name as a variation in the cluster
    - Updates videos.series to canonical_name for all rows with this code
    """
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        # Get cluster info
        cursor.execute(
            "SELECT canonical_name, variations_json FROM series_clusters WHERE id = ?",
            (cluster_id,),
        )
        row = cursor.fetchone()
        if not row:
            return False
        canonical_name = row['canonical_name']
        variations = json.loads(row['variations_json'] or '[]')
        # Get current series of this video (may be empty)
        cursor.execute("SELECT DISTINCT series FROM videos WHERE code = ? AND series IS NOT NULL AND series != ''", (code,))
        old_series_rows = cursor.fetchall()
        for old_row in old_series_rows:
            old_series = old_row['series']
            if old_series and old_series != canonical_name and old_series not in variations:
                variations.append(old_series)
                # Also reassign any other videos with that series name
                cursor.execute(
                    "UPDATE videos SET series = ? WHERE series = ?",
                    (canonical_name, old_series),
                )
        # Update variations_json in cluster
        cursor.execute(
            "UPDATE series_clusters SET variations_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (json.dumps(variations, ensure_ascii=False), cluster_id),
        )
        # Set this video's series to canonical_name
        cursor.execute("UPDATE videos SET series = ? WHERE code = ?", (canonical_name, code))
        conn.commit()
        logger.info(f"✏️ assign_series_cluster [{code}] → cluster {cluster_id} ({canonical_name})")
        return True
    except sqlite3.Error as e:
        logger.error(f"❌ assign_series_cluster 失败: {e}")
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

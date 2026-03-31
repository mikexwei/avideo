import logging
import sqlite3
import sys
from pathlib import Path

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH, LOG_DIR

log_file = LOG_DIR / 'db_init.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()],
)


def init_database() -> None:
    """Initialize and migrate core SQLite schema."""
    logging.info(f'准备初始化/升级数据库: {DB_PATH}')

    pragma_sql = 'PRAGMA foreign_keys = ON;'
    sql_videos = """
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_file_path TEXT UNIQUE NOT NULL,
        code TEXT NOT NULL,
        part TEXT,
        title_jp TEXT,
        title_zh TEXT,
        release_date TEXT,
        duration TEXT,
        maker TEXT,
        publisher TEXT,
        series TEXT,
        score REAL,
        cover_path TEXT,
        symlink_file_path TEXT,
        scrape_status TEXT DEFAULT 'PENDING',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    sql_actors = """
    CREATE TABLE IF NOT EXISTS actors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        is_ignored BOOLEAN DEFAULT 0,
        avatar_path TEXT,
        name_zh TEXT
    );
    """

    sql_tags = """
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    );
    """

    sql_series_clusters = """
    CREATE TABLE IF NOT EXISTS series_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_name TEXT UNIQUE NOT NULL,
        canonical_name_zh TEXT,
        variations_json TEXT,
        is_reviewed INTEGER DEFAULT 0 NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    sql_video_actor = """
    CREATE TABLE IF NOT EXISTS video_actor_link (
        video_id INTEGER,
        actor_id INTEGER,
        PRIMARY KEY (video_id, actor_id),
        FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
        FOREIGN KEY (actor_id) REFERENCES actors(id) ON DELETE CASCADE
    );
    """

    sql_video_tag = """
    CREATE TABLE IF NOT EXISTS video_tag_link (
        video_id INTEGER,
        tag_id INTEGER,
        PRIMARY KEY (video_id, tag_id),
        FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
        FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
    );
    """

    sql_indexes = """
    CREATE INDEX IF NOT EXISTS idx_video_actor_actor_id ON video_actor_link(actor_id);
    CREATE INDEX IF NOT EXISTS idx_video_tag_tag_id ON video_tag_link(tag_id);
    CREATE INDEX IF NOT EXISTS idx_videos_release_date ON videos(release_date);
    CREATE INDEX IF NOT EXISTS idx_series_clusters_is_reviewed ON series_clusters(is_reviewed);
    """

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(pragma_sql)
        cursor.execute(sql_videos)
        cursor.execute(sql_actors)
        cursor.execute(sql_tags)
        cursor.execute(sql_video_actor)
        cursor.execute(sql_video_tag)
        cursor.execute(sql_series_clusters)
        cursor.executescript(sql_indexes)

        sample_males = [('清水健', 1), ('森林原人', 1), ('田渊正浩', 1), ('解禁男', 1)]
        cursor.executemany('INSERT OR IGNORE INTO actors (name, is_ignored) VALUES (?, ?)', sample_males)
        conn.commit()
        logging.info('🎉 数据库表结构升级完毕！多对多关系已建立。')
    except sqlite3.Error as e:
        logging.error(f'❌ 数据库初始化失败: {e}')
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    init_database()

#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

from zhconv import convert

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_PATH


TAGS_TO_DELETE = {'机翻字幕', '机翻字幕 V2'}


def normalize_tags(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute('SELECT id, name FROM tags ORDER BY id').fetchall()
    groups: dict[str, list[tuple[int, str]]] = {}
    for tag_id, name in rows:
        if name not in TAGS_TO_DELETE:
            groups.setdefault(convert(name, 'zh-cn'), []).append((tag_id, name))

    deleted = 0
    merged = 0
    renamed = 0
    with conn:
        for name in TAGS_TO_DELETE:
            row = conn.execute('SELECT id FROM tags WHERE name = ?', (name,)).fetchone()
            if not row:
                continue
            conn.execute('DELETE FROM video_tag_link WHERE tag_id = ?', (row[0],))
            conn.execute('DELETE FROM tags WHERE id = ?', (row[0],))
            deleted += 1

        for simplified, members in groups.items():
            keeper_id, keeper_name = next(
                ((tag_id, name) for tag_id, name in members if name == simplified),
                members[0],
            )
            for tag_id, _name in members:
                if tag_id == keeper_id:
                    continue
                conn.execute(
                    'INSERT OR IGNORE INTO video_tag_link(video_id, tag_id) '
                    'SELECT video_id, ? FROM video_tag_link WHERE tag_id = ?',
                    (keeper_id, tag_id),
                )
                conn.execute('DELETE FROM video_tag_link WHERE tag_id = ?', (tag_id,))
                conn.execute('DELETE FROM tags WHERE id = ?', (tag_id,))
                merged += 1
            if keeper_name != simplified:
                conn.execute('UPDATE tags SET name = ? WHERE id = ?', (simplified, keeper_id))
                renamed += 1

    return {'renamed': renamed, 'merged': merged, 'deleted': deleted}


def main() -> None:
    parser = argparse.ArgumentParser(description='Convert all tags to Simplified Chinese and merge duplicates.')
    parser.add_argument('--db', type=Path, default=DB_PATH)
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    try:
        result = normalize_tags(conn)
    finally:
        conn.close()
    print(f"renamed={result['renamed']} merged={result['merged']} deleted={result['deleted']}")


if __name__ == '__main__':
    main()

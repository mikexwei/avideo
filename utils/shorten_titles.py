from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH
from core.translator.service import DEFAULT_HOST, SHORT_TITLE_MODEL, shorten_chinese_title_with_candidates


def ensure_title_short_column(cursor: sqlite3.Cursor) -> None:
    try:
        cursor.execute("ALTER TABLE videos ADD COLUMN title_zh_short TEXT")
    except sqlite3.OperationalError:
        pass


def title_short_columns(cursor: sqlite3.Cursor) -> set[str]:
    cursor.execute("PRAGMA table_info(videos)")
    return {row[1] for row in cursor.fetchall()}


def _candidate_having_clauses(rebuild: bool, columns: set[str], has_part: bool | None = None) -> list[str]:
    clauses = []
    if not rebuild and "title_zh_short" in columns:
        clauses.append(
            "SUM(CASE WHEN v.title_zh_short IS NULL OR v.title_zh_short = '' THEN 1 ELSE 0 END) > 0 "
            "OR COUNT(DISTINCT COALESCE(v.title_zh_short, '')) > 1"
        )
    if has_part is True:
        clauses.append("SUM(CASE WHEN v.part IS NOT NULL AND v.part != '' THEN 1 ELSE 0 END) > 0")
    elif has_part is False:
        clauses.append("SUM(CASE WHEN v.part IS NOT NULL AND v.part != '' THEN 1 ELSE 0 END) = 0")
    return clauses


def load_candidates(
    cursor: sqlite3.Cursor,
    count: int | None,
    rebuild: bool,
    columns: set[str],
    has_part: bool | None = None,
    exclude_codes: set[str] | None = None,
) -> list[sqlite3.Row]:
    having_clauses = _candidate_having_clauses(rebuild, columns, has_part=has_part)
    having_clause = f"HAVING {' AND '.join(f'({clause})' for clause in having_clauses)}" if having_clauses else ""
    exclude_codes = exclude_codes or set()
    exclude_clause = ""
    exclude_params: list[str] = []
    if exclude_codes:
        exclude_clause = f"AND v.code NOT IN ({','.join('?' for _ in exclude_codes)})"
        exclude_params = list(exclude_codes)
    limit_clause = "LIMIT ?" if count is not None else ""
    params = exclude_params + ([count] if count is not None else [])
    cursor.execute(
        f"""
        SELECT v.code,
               COUNT(*) AS row_count,
               GROUP_CONCAT(NULLIF(v.part, ''), '|') AS parts,
               (
                   SELECT v2.title_zh
                   FROM videos v2
                   WHERE v2.deleted = 0
                     AND v2.code = v.code
                     AND v2.title_zh IS NOT NULL
                     AND v2.title_zh != ''
                   ORDER BY v2.id
                   LIMIT 1
               ) AS title_zh,
               (
                   SELECT GROUP_CONCAT(actor_name, '|')
                   FROM (
                       SELECT DISTINCT COALESCE(a.name_zh, a.name) AS actor_name, a.id AS actor_id
                       FROM videos vx
                       JOIN video_actor_link val ON val.video_id = vx.id
                       JOIN actors a ON a.id = val.actor_id
                       WHERE vx.deleted = 0
                         AND vx.code = v.code
                         AND COALESCE(a.name_zh, a.name) IS NOT NULL
                         AND COALESCE(a.name_zh, a.name) != ''
                       ORDER BY actor_id
                   )
               ) AS actor_names
        FROM videos v
        WHERE v.deleted = 0
          AND v.title_zh IS NOT NULL
          AND v.title_zh != ''
          {exclude_clause}
        GROUP BY v.code
        {having_clause}
        ORDER BY RANDOM()
        {limit_clause}
        """,
        params,
    )
    return cursor.fetchall()


def load_dryrun_candidates(cursor: sqlite3.Cursor, count: int | None, rebuild: bool, columns: set[str]) -> list[sqlite3.Row]:
    if count is None or count < 2:
        return load_candidates(cursor, count, rebuild, columns)

    part_count = count // 2
    no_part_count = count - part_count
    part_rows = load_candidates(cursor, part_count, rebuild, columns, has_part=True)
    no_part_rows = load_candidates(cursor, no_part_count, rebuild, columns, has_part=False)

    rows = part_rows + no_part_rows
    if len(rows) < count:
        used_codes = {row["code"] for row in rows}
        rows.extend(load_candidates(cursor, count - len(rows), rebuild, columns, exclude_codes=used_codes))
    return rows


def batch_shorten_titles(
    db_path: Path = DB_PATH,
    host: str = DEFAULT_HOST,
    model: str = SHORT_TITLE_MODEL,
    dryrun: bool = False,
    count: int | None = None,
    rebuild: bool = False,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    columns = title_short_columns(cursor)
    if not dryrun and "title_zh_short" not in columns:
        ensure_title_short_column(cursor)
        conn.commit()
        columns = title_short_columns(cursor)

    rows = load_dryrun_candidates(cursor, count, rebuild, columns) if dryrun else load_candidates(cursor, count, rebuild, columns)
    if not rows:
        print("没有需要生成缩略标题的记录。")
        conn.close()
        return

    print(f"=== 生成中文缩略标题: {len(rows)} 个作品组, model={model}, dryrun={dryrun} ===")
    updated = 0
    for idx, row in enumerate(rows, 1):
        actor_names = [n for n in (row["actor_names"] or "").split("|") if n]
        parts = [p for p in (row["parts"] or "").split("|") if p]
        start = time.time()
        short_title, candidates = shorten_chinese_title_with_candidates(
            row["title_zh"],
            actor_names=actor_names,
            host=host,
            model=model,
            max_len=20,
        )
        elapsed = time.time() - start

        print(f"[{idx}/{len(rows)}] {row['code']}")
        print(f"  行数: {row['row_count']}")
        if parts:
            print(f"  分段: {', '.join(parts)}")
        print(f"  原标题: {row['title_zh']}")
        print(f"  女优: {', '.join(actor_names) if actor_names else '无'}")
        if dryrun:
            print(f"  候选: {', '.join(candidates) if candidates else '无'}")
        print(f"  缩略: {short_title} ({elapsed:.1f}s)")

        if not dryrun:
            cursor.execute(
                "UPDATE videos SET title_zh_short = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?",
                (short_title, row["code"]),
            )
            updated += cursor.rowcount
            conn.commit()

    conn.close()
    if dryrun:
        print(f"DRYRUN 完成: 预览 {len(rows)} 个作品组，未更新数据库。")
    else:
        print(f"完成: 更新 {updated} 条记录。")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate short Chinese video titles with Ollama.")
    parser.add_argument("--dryrun", action="store_true", help="Print generated short titles without updating DB.")
    parser.add_argument("--count", type=int, default=None, help="Randomly process N titles.")
    parser.add_argument("--rebuild", action="store_true", help="Regenerate titles that already have title_zh_short.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Ollama host, default: {DEFAULT_HOST}")
    parser.add_argument("--model", default=SHORT_TITLE_MODEL, help=f"Ollama model, default: {SHORT_TITLE_MODEL}")
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be a positive integer")

    batch_shorten_titles(
        host=args.host,
        model=args.model,
        dryrun=args.dryrun,
        count=args.count,
        rebuild=args.rebuild,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

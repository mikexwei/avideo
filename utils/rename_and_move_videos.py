import argparse
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH


SCAN_ROOT = Path("/Volumes/T73")
DEST_ROOT = SCAN_ROOT / "avideo"
FIELD_SEP = "\x1f"
INVALID_COMPONENT_CHARS = re.compile(r"[\x00-\x1f/:]+")


@dataclass
class VideoRecord:
    id: int
    code: str
    part: Optional[str]
    original_file_path: Path
    title_zh_short: Optional[str]
    title_zh: Optional[str]
    title_jp: Optional[str]
    actors: list[str]


@dataclass
class MovePlan:
    record: VideoRecord
    source: Path
    dest: Path
    status: str
    reason: str = ""


def sanitize_component(value: str) -> str:
    cleaned = INVALID_COMPONENT_CHARS.sub(" ", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.strip(". ")


def code_prefix(code: str) -> str:
    return sanitize_component((code or "").split("-", 1)[0])


def build_filename(record: VideoRecord) -> str:
    ext = record.original_file_path.suffix
    components = [sanitize_component(record.code)]
    components.extend(name for name in (sanitize_component(a) for a in record.actors) if name)

    title = sanitize_component(record.title_zh_short or record.title_zh or record.title_jp or "")
    if title:
        components.append(f"[{title}]")
    if record.part:
        components.append(sanitize_component(record.part))

    return ".".join(c for c in components if c) + ext


def build_destination(record: VideoRecord, dest_root: Path = DEST_ROOT) -> Path:
    return dest_root / code_prefix(record.code) / build_filename(record)


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_records(db_path: Path, scan_root: Path = SCAN_ROOT, count: Optional[int] = None) -> list[VideoRecord]:
    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        sql = """
            SELECT id, code, part, original_file_path, title_zh_short, title_zh, title_jp
            FROM videos
            WHERE deleted = 0
              AND (original_file_path = ? OR original_file_path LIKE ?)
            ORDER BY id ASC
        """
        params: list = [str(scan_root), str(scan_root) + "/%"]
        if count is not None:
            sql += " LIMIT ?"
            params.append(count)
        cursor.execute(sql, params)
        rows = cursor.fetchall()

        records = []
        for row in rows:
            source = Path(row["original_file_path"])
            if not _is_under(source, scan_root):
                continue
            cursor.execute(
                """
                SELECT COALESCE(a.name_zh, a.name) AS name
                FROM actors a
                JOIN video_actor_link val ON val.actor_id = a.id
                WHERE val.video_id = ?
                ORDER BY a.id ASC
                """,
                (row["id"],),
            )
            actors = [r["name"] for r in cursor.fetchall() if r["name"]]
            records.append(
                VideoRecord(
                    id=row["id"],
                    code=row["code"],
                    part=row["part"],
                    original_file_path=source,
                    title_zh_short=row["title_zh_short"],
                    title_zh=row["title_zh"],
                    title_jp=row["title_jp"],
                    actors=actors,
                )
            )
        return records
    finally:
        conn.close()


def plan_move(record: VideoRecord, db_path: Path, dest_root: Path = DEST_ROOT) -> MovePlan:
    source = record.original_file_path
    dest = build_destination(record, dest_root)

    if not source.exists():
        return MovePlan(record, source, dest, "skip", "source missing")
    if source == dest:
        return MovePlan(record, source, dest, "skip", "already correct")
    if dest.exists():
        return MovePlan(record, source, dest, "skip", "destination exists")

    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM videos WHERE original_file_path = ? AND id != ? LIMIT 1",
            (str(dest), record.id),
        )
        if cursor.fetchone():
            return MovePlan(record, source, dest, "skip", "destination path already in DB")
    finally:
        conn.close()

    return MovePlan(record, source, dest, "move")


def execute_move(plan: MovePlan, db_path: Path) -> MovePlan:
    if plan.status != "move":
        return plan

    plan.dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(plan.source), str(plan.dest))

    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (str(plan.dest), plan.record.id),
        )
        if cursor.rowcount != 1:
            raise sqlite3.Error("database row not found")
        conn.commit()
        return MovePlan(plan.record, plan.source, plan.dest, "moved")
    except Exception as exc:
        conn.rollback()
        try:
            shutil.move(str(plan.dest), str(plan.source))
        except Exception as rollback_exc:
            return MovePlan(plan.record, plan.source, plan.dest, "error", f"{exc}; rollback failed: {rollback_exc}")
        return MovePlan(plan.record, plan.source, plan.dest, "error", str(exc))
    finally:
        conn.close()


def run(dryrun: bool, count: Optional[int], db_path: Path = DB_PATH, scan_root: Path = SCAN_ROOT, dest_root: Path = DEST_ROOT) -> int:
    records = load_records(db_path, scan_root, count)
    plans = [plan_move(record, db_path, dest_root) for record in records]

    moved = skipped = errors = 0
    for plan in plans:
        if dryrun:
            if plan.status == "move":
                print(f"MOVE: {plan.source} -> {plan.dest}")
            else:
                skipped += 1
                print(f"SKIP: {plan.reason} | {plan.source} -> {plan.dest}")
            continue

        result = execute_move(plan, db_path)
        if result.status == "moved":
            moved += 1
            print(f"MOVED: {result.source} -> {result.dest}")
        elif result.status == "skip":
            skipped += 1
            print(f"SKIP: {result.reason} | {result.source} -> {result.dest}")
        else:
            errors += 1
            print(f"ERROR: {result.reason} | {result.source} -> {result.dest}")

    if dryrun:
        print(f"SUMMARY: candidates={len(plans)}, dryrun_moves={sum(1 for p in plans if p.status == 'move')}, skipped={skipped}")
    else:
        print(f"SUMMARY: candidates={len(plans)}, moved={moved}, skipped={skipped}, errors={errors}")
    return 1 if errors else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Rename and move videos under /Volumes/12TB into /Volumes/12TB/avideo/<prefix>/")
    parser.add_argument("--dryrun", action="store_true", help="Print planned moves without moving files or updating DB.")
    parser.add_argument("--count", type=int, default=None, help="Only process the first N DB records by videos.id.")
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be a positive integer")

    return run(dryrun=args.dryrun, count=args.count)


if __name__ == "__main__":
    raise SystemExit(main())

import sqlite3
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from utils.rename_and_move_videos import (
    VideoRecord,
    build_destination,
    build_filename,
    execute_move,
    load_records,
    plan_move,
    run,
    sanitize_component,
)


def init_test_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_file_path TEXT UNIQUE NOT NULL,
            code TEXT NOT NULL,
            part TEXT,
            title_jp TEXT,
            title_zh TEXT,
            title_zh_short TEXT,
            deleted INTEGER DEFAULT 0,
            updated_at TEXT
        );
        CREATE TABLE actors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            name_zh TEXT
        );
        CREATE TABLE video_actor_link (
            video_id INTEGER,
            actor_id INTEGER,
            PRIMARY KEY (video_id, actor_id)
        );
        """
    )
    conn.commit()
    return conn


class RenameAndMoveVideosTests(unittest.TestCase):
    def test_build_filename_keeps_suffix_actors_title_and_part(self):
        record = VideoRecord(
            id=1,
            code="SSIS-698-UC",
            part="part1",
            original_file_path=Path("/tmp/source.mp4"),
            title_zh_short="精美写真三张",
            title_zh="和新有菜与相泽南 精美写真三张",
            title_jp="jp title",
            actors=["三上悠亚", "新有菜", "相泽南"],
        )

        self.assertEqual(
            build_filename(record),
            "SSIS-698-UC.三上悠亚.新有菜.相泽南.[精美写真三张].part1.mp4",
        )

    def test_build_filename_falls_back_to_japanese_title(self):
        record = VideoRecord(1, "ABP-123-C", None, Path("/tmp/source.mkv"), None, None, "日本語", [])
        self.assertEqual(build_filename(record), "ABP-123-C.[日本語].mkv")

    def test_sanitize_component_removes_invalid_chars(self):
        self.assertEqual(sanitize_component("a/b:c\n d"), "a b c d")

    def test_build_destination_uses_code_prefix_directory(self):
        record = VideoRecord(1, "SSIS-698-UC", None, Path("/tmp/source.mp4"), None, None, None, [])
        self.assertEqual(
            build_destination(record, Path("/Volumes/12TB/avideo")),
            Path("/Volumes/12TB/avideo/SSIS/SSIS-698-UC.mp4"),
        )

    def test_dryrun_does_not_move_or_update_db(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "test.db"
            source = root / "raw.mp4"
            source.write_bytes(b"video")
            conn = init_test_db(db_path)
            conn.execute(
                "INSERT INTO videos (id, original_file_path, code, title_zh, title_zh_short) VALUES (1, ?, 'ABP-123-C', '长标题', '短标题')",
                (str(source),),
            )
            conn.commit()
            conn.close()

            with redirect_stdout(StringIO()):
                exit_code = run(True, None, db_path=db_path, scan_root=root, dest_root=root / "avideo")

            self.assertEqual(exit_code, 0)
            self.assertTrue(source.exists())
            self.assertFalse((root / "avideo").exists())
            conn = sqlite3.connect(db_path)
            stored_path = conn.execute("SELECT original_file_path FROM videos WHERE id = 1").fetchone()[0]
            conn.close()
            self.assertEqual(stored_path, str(source))

    def test_execute_move_updates_db(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "test.db"
            source = root / "raw.mp4"
            source.write_bytes(b"video")
            conn = init_test_db(db_path)
            conn.execute(
                "INSERT INTO videos (id, original_file_path, code, title_zh, title_zh_short) VALUES (1, ?, 'ABP-123-C', '长标题', '短标题')",
                (str(source),),
            )
            conn.commit()
            conn.close()

            record = load_records(db_path, root)[0]
            plan = plan_move(record, db_path, root / "avideo")
            result = execute_move(plan, db_path)

            expected = root / "avideo" / "ABP" / "ABP-123-C.[短标题].mp4"
            self.assertEqual(result.status, "moved")
            self.assertFalse(source.exists())
            self.assertTrue(expected.exists())
            conn = sqlite3.connect(db_path)
            stored_path = conn.execute("SELECT original_file_path FROM videos WHERE id = 1").fetchone()[0]
            conn.close()
            self.assertEqual(stored_path, str(expected))

    def test_destination_conflict_skips_without_db_update(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "test.db"
            source = root / "raw.mp4"
            source.write_bytes(b"video")
            dest_dir = root / "avideo" / "ABP"
            dest_dir.mkdir(parents=True)
            dest = dest_dir / "ABP-123-C.[短标题].mp4"
            dest.write_bytes(b"existing")
            conn = init_test_db(db_path)
            conn.execute(
                "INSERT INTO videos (id, original_file_path, code, title_zh, title_zh_short) VALUES (1, ?, 'ABP-123-C', '长标题', '短标题')",
                (str(source),),
            )
            conn.commit()
            conn.close()

            record = load_records(db_path, root)[0]
            plan = plan_move(record, db_path, root / "avideo")

            self.assertEqual(plan.status, "skip")
            self.assertEqual(plan.reason, "destination exists")
            self.assertTrue(source.exists())


if __name__ == "__main__":
    unittest.main()

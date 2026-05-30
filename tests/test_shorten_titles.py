import sqlite3
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from core.translator.service import clean_short_title
from utils.shorten_titles import batch_shorten_titles


def init_db(db_path: Path, include_short_column: bool = False):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            part TEXT,
            title_zh TEXT,
            deleted INTEGER DEFAULT 0,
            updated_at TEXT
        );
        CREATE TABLE actors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            name_zh TEXT
        );
        CREATE TABLE video_actor_link (
            video_id INTEGER,
            actor_id INTEGER
        );
        """
    )
    if include_short_column:
        cursor.execute("ALTER TABLE videos ADD COLUMN title_zh_short TEXT")
    cursor.execute("INSERT INTO videos (id, code, title_zh) VALUES (1, 'SSIS-698-UC', '三上悠亚和新有菜与相泽南精美写真三张')")
    cursor.execute("INSERT INTO actors (id, name, name_zh) VALUES (1, 'Mikami Yua', '三上悠亚')")
    cursor.execute("INSERT INTO actors (id, name, name_zh) VALUES (2, 'Arina', '新有菜')")
    cursor.execute("INSERT INTO video_actor_link (video_id, actor_id) VALUES (1, 1)")
    cursor.execute("INSERT INTO video_actor_link (video_id, actor_id) VALUES (1, 2)")
    conn.commit()
    conn.close()


class ShortenTitlesTests(unittest.TestCase):
    def test_clean_short_title_removes_spaces_and_punctuation_and_limits_length(self):
        self.assertEqual(clean_short_title(" 三上悠亚，精美 写真!!!abcdef0123456789", 20), "三上悠亚精美写真abcdef012345")

    @patch("utils.shorten_titles.shorten_chinese_title_with_candidates", return_value=("精美写真三张", ["精美写真三张", "写真特典三张", "三人写真特典"]))
    def test_dryrun_does_not_update_db(self, _mock_shorten):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            init_db(db_path, include_short_column=True)

            batch_shorten_titles(db_path=db_path, dryrun=True, count=1)

            conn = sqlite3.connect(db_path)
            value = conn.execute("SELECT title_zh_short FROM videos WHERE id = 1").fetchone()[0]
            conn.close()
            self.assertIsNone(value)

    @patch("utils.shorten_titles.shorten_chinese_title_with_candidates", return_value=("精美写真三张", ["精美写真三张"]))
    def test_dryrun_does_not_add_column_when_missing(self, _mock_shorten):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            init_db(db_path)

            batch_shorten_titles(db_path=db_path, dryrun=True, count=1)

            conn = sqlite3.connect(db_path)
            columns = [row[1] for row in conn.execute("PRAGMA table_info(videos)").fetchall()]
            conn.close()
            self.assertNotIn("title_zh_short", columns)

    @patch("utils.shorten_titles.shorten_chinese_title_with_candidates", return_value=("精美写真三张", ["精美写真三张"]))
    def test_real_run_updates_short_title(self, _mock_shorten):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            init_db(db_path)

            batch_shorten_titles(db_path=db_path, dryrun=False, count=1)

            conn = sqlite3.connect(db_path)
            value = conn.execute("SELECT title_zh_short FROM videos WHERE id = 1").fetchone()[0]
            conn.close()
            self.assertEqual(value, "精美写真三张")

    @patch("utils.shorten_titles.shorten_chinese_title_with_candidates", return_value=("精美写真三张", ["精美写真三张"]))
    def test_real_run_updates_all_parts_for_same_code_with_one_model_call(self, mock_shorten):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            init_db(db_path, include_short_column=True)

            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE videos SET part = 'part1' WHERE id = 1")
            conn.execute(
                "INSERT INTO videos (id, code, part, title_zh) VALUES (2, 'SSIS-698-UC', 'part2', '新有菜和三上悠亚精美写真三张')"
            )
            conn.execute("INSERT INTO video_actor_link (video_id, actor_id) VALUES (2, 1)")
            conn.execute("INSERT INTO video_actor_link (video_id, actor_id) VALUES (2, 2)")
            conn.commit()
            conn.close()

            batch_shorten_titles(db_path=db_path, dryrun=False, count=1)

            conn = sqlite3.connect(db_path)
            values = conn.execute(
                "SELECT title_zh_short FROM videos WHERE code = 'SSIS-698-UC' ORDER BY id"
            ).fetchall()
            conn.close()
            self.assertEqual([row[0] for row in values], ["精美写真三张", "精美写真三张"])
            self.assertEqual(mock_shorten.call_count, 1)

    @patch("utils.shorten_titles.shorten_chinese_title_with_candidates", return_value=("统一短标题", ["统一短标题"]))
    def test_inconsistent_existing_titles_are_selected_without_rebuild(self, _mock_shorten):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            init_db(db_path, include_short_column=True)

            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE videos SET part = 'part1', title_zh_short = '短标题一' WHERE id = 1")
            conn.execute(
                "INSERT INTO videos (id, code, part, title_zh, title_zh_short) VALUES (2, 'SSIS-698-UC', 'part2', '新有菜和三上悠亚精美写真三张', '短标题二')"
            )
            conn.commit()
            conn.close()

            batch_shorten_titles(db_path=db_path, dryrun=False, count=1)

            conn = sqlite3.connect(db_path)
            values = conn.execute(
                "SELECT DISTINCT title_zh_short FROM videos WHERE code = 'SSIS-698-UC'"
            ).fetchall()
            conn.close()
            self.assertEqual([row[0] for row in values], ["统一短标题"])

    @patch("utils.shorten_titles.shorten_chinese_title_with_candidates", return_value=("精美写真三张", ["精美写真三张", "写真特典三张", "三人写真特典"]))
    def test_dryrun_prints_model_candidates(self, _mock_shorten):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            init_db(db_path, include_short_column=True)

            output = StringIO()
            with redirect_stdout(output):
                batch_shorten_titles(db_path=db_path, dryrun=True, count=1)

            self.assertIn("候选: 精美写真三张, 写真特典三张, 三人写真特典", output.getvalue())

    @patch("utils.shorten_titles.shorten_chinese_title_with_candidates", return_value=("短标题", ["短标题"]))
    def test_dryrun_count_samples_part_and_non_part_groups(self, _mock_shorten):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            init_db(db_path, include_short_column=True)

            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE videos SET code = 'PART-001', part = 'part1' WHERE id = 1")
            conn.execute("INSERT INTO videos (id, code, title_zh) VALUES (2, 'NOPART-001', '温泉旅行中的秘密约会')")
            conn.commit()
            conn.close()

            output = StringIO()
            with redirect_stdout(output):
                batch_shorten_titles(db_path=db_path, dryrun=True, count=2)

            text = output.getvalue()
            self.assertIn("PART-001", text)
            self.assertIn("NOPART-001", text)


if __name__ == "__main__":
    unittest.main()

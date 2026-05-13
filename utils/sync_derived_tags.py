import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from dal.db_manager import sync_all_derived_video_tags


if __name__ == "__main__":
    changed = sync_all_derived_video_tags()
    print(f"同步完成，写入 {changed} 个派生标签关联。")

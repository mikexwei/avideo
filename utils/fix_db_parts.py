import sqlite3
import sys
from pathlib import Path

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH
from core.scanner import extract_video_code

def fix_db_parts():
    if not DB_PATH.exists():
        print(f"❌ 数据库不存在: {DB_PATH}")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 查询所有视频记录
        cursor.execute("SELECT id, original_file_path, code, part FROM videos")
        records = cursor.fetchall()

        if not records:
            print("数据库中没有视频记录。")
            return

        update_count = 0
        print(f"🔍 正在检查 {len(records)} 条数据库记录...\n")

        for record_id, file_path, old_code, old_part in records:
            if not file_path:
                continue
            
            # 提取文件名 (不包含扩展名)
            stem = Path(file_path).stem
            
            # 调用最新版的正则提取逻辑
            new_code, new_part = extract_video_code(stem)
            
            if not new_code:
                continue

            # ================= 新增二次清洗逻辑 =================
            # 1. 将字母分集转换为数字分集 (例如: parta -> part1, partc -> part3)
            if new_part and new_part.startswith('part') and len(new_part) == 5:
                p_char = new_part[4].lower()
                if p_char in 'abcdef':
                    char_map = {'a': '1', 'b': '2', 'c': '3', 'd': '4', 'e': '5', 'f': '6'}
                    new_part = f"part{char_map[p_char]}"
                    
            # 2. 如果番号中包含 VR，且以 -C 结尾，则强制去除 -C
            if new_code and 'VR' in new_code.upper() and new_code.upper().endswith('-C'):
                new_code = new_code[:-2]
            # ====================================================

            # 检查是否需要更新 (将 None 统一视为空字符串以便对比)
            if (old_code or "") != (new_code or "") or (old_part or "") != (new_part or ""):
                cursor.execute(
                    "UPDATE videos SET code = ?, part = ? WHERE id = ?",
                    (new_code, new_part, record_id)
                )
                update_count += 1
                print(f"🔄 更新 ID:{record_id} | 文件: {Path(file_path).name}")
                print(f"   ↳ 番号: [{old_code or '无'}] -> [{new_code or '无'}], 分集: [{old_part or '无'}] -> [{new_part or '无'}]")

        conn.commit()
        print(f"\n🎉 修正完毕！共扫描 {len(records)} 条，修正了 {update_count} 条记录的分集/番号信息。")

    except sqlite3.Error as e:
        print(f"❌ 数据库操作失败: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    fix_db_parts()

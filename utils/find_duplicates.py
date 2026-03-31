import sqlite3
import unicodedata
import sys
from pathlib import Path

# 动态引入 config 模块
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH

def get_display_width(text: str) -> int:
    """
    计算字符串在终端中的实际视觉宽度
    中日韩字符 (W, F) 计为 2 个宽度，其他算 1 个
    """
    if not text:
        return 0
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in str(text))

def pad_text(text: str, target_width: int) -> str:
    """使用空格填充文本，直到达到指定的视觉宽度"""
    text = str(text) if text is not None else ""
    padding = target_width - get_display_width(text)
    return text + " " * max(0, padding)

def find_and_print_duplicates():
    if not DB_PATH.exists():
        print(f"❌ 数据库不存在: {DB_PATH}")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 核心 SQL：子查询找出所有重复的 (code, part) 组合，主查询捞出详情，并排序
        sql = """
            SELECT original_file_path, code, part
            FROM videos
            WHERE (code, IFNULL(part, '')) IN (
                SELECT code, IFNULL(part, '')
                FROM videos
                GROUP BY code, IFNULL(part, '')
                HAVING COUNT(id) > 1
            )
            ORDER BY code, part, original_file_path
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        if not results:
            print("🎉 恭喜，数据库中没有任何重复的 Code + Part 组合！")
            return

        # 第一遍遍历：收集数据并动态计算各列需要的最大视觉宽度
        processed_data = []
        max_filename_w = 9   # "File Name" 的基础宽度
        max_code_w = 4       # "Code" 的基础宽度
        max_part_w = 4       # "Part" 的基础宽度

        for row in results:
            file_path, code, part = row
            # 仅提取文件名，忽略冗长的前置目录
            filename = Path(file_path).name if file_path else "Unknown"
            code_str = str(code) if code else ""
            part_str = str(part) if part else ""

            max_filename_w = max(max_filename_w, get_display_width(filename))
            max_code_w = max(max_code_w, get_display_width(code_str))
            max_part_w = max(max_part_w, get_display_width(part_str))

            processed_data.append({'filename': filename, 'code': code_str, 'part': part_str})

        # 为每列增加间隔缓冲
        max_filename_w += 4
        max_code_w += 4

        # 第二遍遍历：打印输出
        print("\n" + pad_text("File Name", max_filename_w) + pad_text("Code", max_code_w) + "Part")
        print("=" * (max_filename_w + max_code_w + max_part_w))

        current_group = None
        for item in processed_data:
            # 当 code+part 组合切换时，画一条分割线，形成清晰的分组视觉
            group_key = (item['code'], item['part'])
            if current_group is not None and current_group != group_key:
                print("-" * (max_filename_w + max_code_w + max_part_w))
            
            print(pad_text(item['filename'], max_filename_w) + pad_text(item['code'], max_code_w) + item['part'])
            current_group = group_key

        print(f"\n📊 统计: 发现 {len(processed_data)} 个文件，归属于 {len(set((item['code'], item['part']) for item in processed_data))} 个不同的重复组。")

    except sqlite3.Error as e:
        print(f"❌ 数据库查询失败: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    find_and_print_duplicates()

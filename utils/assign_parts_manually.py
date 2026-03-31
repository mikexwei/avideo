import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH

def interactive_assign_parts():
    if not DB_PATH.exists():
        print(f"❌ 数据库不存在: {DB_PATH}")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 1. 查找重复项 (基于 code + part 组合)
        sql = """
            SELECT id, original_file_path, code, part
            FROM videos
            WHERE (code, IFNULL(part, '')) IN (
                SELECT code, IFNULL(part, '')
                FROM videos
                GROUP BY code, IFNULL(part, '')
                HAVING COUNT(id) > 1
            )
            ORDER BY code, IFNULL(part, ''), original_file_path
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        if not results:
            print("🎉 数据库中没有任何重复的 Code + Part 组合！")
            return

        # 2. 分组数据
        groups = defaultdict(list)
        for row in results:
            record_id, file_path, code, part = row
            filename = Path(file_path).name if file_path else "Unknown"
            group_key = (code, part)
            groups[group_key].append({
                'id': record_id,
                'filename': filename,
                'code': code,
                'part': part
            })

        print(f"🔍 发现 {len(groups)} 组重复数据。")
        print("=" * 70)
        
        # 3. 循环展示并询问
        for (code, part), records in groups.items():
            print(f"\n📁 重复组: Code = {code} | Part = {part or '无'}")
            print("-" * 50)
            
            # 按照文件名进行字母排序
            records.sort(key=lambda x: x['filename'])
            
            for item in records:
                print(f"  📄 文件名: {item['filename']} | Code: {item['code']} | 当前Part: {item['part'] or '无'}")
            
            print("-" * 50)
            ans = input("❓ 是否为该组文件按顺序重新分配 Part (part1, part2...) ? (y/N): ").strip().lower()
            
            if ans == 'y':
                for index, item in enumerate(records, start=1):
                    new_part = f"part{index}"
                    print(f"  [已更新] 文件名: {item['filename']} -> 分配新 Part: {new_part}")
                    cursor.execute("UPDATE videos SET part = ? WHERE id = ?", (new_part, item['id']))
                conn.commit()
                print("  ✅ 数据库已保存此组修改！")
            else:
                print("  ⏭️ 已跳过。")

        print("\n🎉 所有重复组处理完毕！")

    except sqlite3.Error as e:
        print(f"❌ 数据库操作失败: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("=== 🛠️  交互式修复重复 Part 工具 ===")
    interactive_assign_parts()

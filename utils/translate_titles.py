import sqlite3
import time
import sys
from pathlib import Path

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH
from core.translator.service import translate_title, DEFAULT_HOST, DEFAULT_MODEL


def batch_translate_titles(host: str = DEFAULT_HOST, model: str = DEFAULT_MODEL, retranslate: bool = False):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    where_extra = "" if retranslate else "AND (v.title_zh IS NULL OR v.title_zh = '')"
    sql = f"""
        SELECT v.title_jp,
               GROUP_CONCAT(DISTINCT v.code) as codes,
               GROUP_CONCAT(a.name || '|' || COALESCE(a.name_zh, '')) as actor_pairs
        FROM videos v
        LEFT JOIN video_actor_link val ON v.id = val.video_id
        LEFT JOIN actors a ON val.actor_id = a.id
        WHERE v.title_jp IS NOT NULL
          AND v.title_jp != ''
          {where_extra}
        GROUP BY v.title_jp
    """
    cursor.execute(sql)
    pending = cursor.fetchall()

    if not pending:
        print("🎉 没有需要翻译的视频标题。")
        conn.close()
        return

    SEP = '-' * 72
    print(f"=== 开始翻译 (共 {len(pending)} 条, model={model}) ===")

    success_count = 0
    for idx, (title_jp, codes, actor_pairs) in enumerate(pending, 1):
        code_list = [c.strip() for c in (codes or '').split(',') if c.strip()]

        # 构建日文名->中文名映射（只保留有中文名的演员）
        actor_name_map: dict = {}
        for pair in (actor_pairs or '').split(','):
            pair = pair.strip()
            if '|' in pair:
                jp, zh = pair.split('|', 1)
                jp, zh = jp.strip(), zh.strip()
                if jp and zh:
                    actor_name_map[jp] = zh

        try:
            start = time.time()
            title_zh = translate_title(title_jp, codes=code_list, actor_name_map=actor_name_map, host=host, model=model)
            elapsed = time.time() - start
        except Exception as e:
            print(f"\n[{idx}/{len(pending)}] ERROR: {e}")
            print(SEP)
            continue

        label_w = 4
        print(f"\n[{idx}/{len(pending)}]")
        print(f"  {'JP':<{label_w}} {title_jp}")
        print(f"  {'ZH':<{label_w}} {title_zh}  ({elapsed:.1f}s)")
        print(SEP)

        cursor.execute("UPDATE videos SET title_zh = ? WHERE title_jp = ?", (title_zh, title_jp))
        conn.commit()
        success_count += cursor.rowcount

    conn.close()
    print(f"\n完成: 翻译 {len(pending)} 条，更新 {success_count} 条记录。")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default=DEFAULT_HOST, help='Ollama 服务地址')
    parser.add_argument('--model', default=DEFAULT_MODEL, help='模型名称')
    parser.add_argument('--retranslate', action='store_true', help='强制重新翻译所有（包括已有译文的）')
    args = parser.parse_args()
    batch_translate_titles(host=args.host, model=args.model, retranslate=args.retranslate)

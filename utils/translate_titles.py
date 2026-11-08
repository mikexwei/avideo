import sqlite3
import sys
from pathlib import Path

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH
from core.translator.service import DEFAULT_HOST, DEFAULT_MODEL


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

    from core.worker_client import WorkerClient, store
    client = WorkerClient()
    active = {(task['kind'], task['code']) for task in store().active_tasks()}
    tasks = []
    for title_jp, codes, actor_pairs in pending:
        code_list = [code.strip() for code in (codes or '').split(',') if code.strip()]
        actor_name_map = {}
        for pair in (actor_pairs or '').split(','):
            if '|' in pair:
                jp, zh = pair.split('|', 1)
                if jp.strip() and zh.strip():
                    actor_name_map[jp.strip()] = zh.strip()
        for code in code_list:
            if ('translate', code) in active:
                continue
            previous = conn.execute("SELECT COALESCE(title_zh,'') FROM videos WHERE code=? LIMIT 1", (code,)).fetchone()
            tasks.append(client.call('translate', code, {'title': title_jp, 'codes': [code],
                'actor_name_map': actor_name_map, 'previous_titles': {code: previous[0] if previous else ''},
                'options': {'host': host, 'model': model}}))
            active.add(('translate', code))
    conn.close()
    print(f'已提交 {len(tasks)} 个翻译任务，可在 /admin 查看进度。')
    return tasks


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default=DEFAULT_HOST, help='Ollama 服务地址')
    parser.add_argument('--model', default=DEFAULT_MODEL, help='模型名称')
    parser.add_argument('--retranslate', action='store_true', help='强制重新翻译所有（包括已有译文的）')
    args = parser.parse_args()
    batch_translate_titles(host=args.host, model=args.model, retranslate=args.retranslate)

import sqlite3
import time
import sys
import re
from pathlib import Path
from ollama import Client

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH

def batch_translate_titles():
    # 初始化 Ollama 客户端
    mac_client = Client(host='http://10.0.0.40:11434')
    model_name = 'quantumcookie/Sakura-qwen2.5-v1.0:14b' 

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 查找所有需要翻译的视频 (按 title_jp 去重，避免同名分集重复翻译)
    sql = """
        SELECT v.title_jp, 
               GROUP_CONCAT(v.code) as codes,
               GROUP_CONCAT(a.name) as actor_names,
               GROUP_CONCAT(a.name_zh) as actor_names_zh
        FROM videos v
        LEFT JOIN video_actor_link val ON v.id = val.video_id
        LEFT JOIN actors a ON val.actor_id = a.id
        WHERE v.title_jp IS NOT NULL 
          AND v.title_jp != ''
        GROUP BY v.title_jp
    """
    cursor.execute(sql)
    pending_videos = cursor.fetchall()
    
    if not pending_videos:
        print("🎉 太棒了！数据库中没有任何需要翻译的视频标题。")
        conn.close()
        return

    print(f"=== 🚀 开始全自动大模型标题翻译引擎 (共 {len(pending_videos)} 条待处理) ===")
    print("=" * 80)
    
    success_count = 0
    
    for idx, (original_title, codes, actor_names, actor_names_zh) in enumerate(pending_videos, 1):
        print(f"[{idx}/{len(pending_videos)}] 正在处理去重后的标题...")
        print(f"🇯🇵 原标: {original_title}")
        
        clean_title = original_title
        
        # 0. 深度清洗：去除标题中包含的番号
        if codes:
            code_list = set()
            for c in codes.split(','):
                c = c.strip()
                if c:
                    # 去掉本地可能附加的特征后缀 (-C, -U 等)，还原纯净番号
                    base_c = re.sub(r'-[cur]+$', '', c, flags=re.IGNORECASE)
                    code_list.add(base_c)
                    code_list.add(base_c.replace('-', '')) # 例如 ABC-123 和 ABC123 都要删掉
            for c in sorted(list(code_list), key=len, reverse=True):
                clean_title = re.sub(re.escape(c), '', clean_title, flags=re.IGNORECASE)
        
        # 1. 深度清洗：预处理标签和符号
        clean_title = re.sub(r'【.*?】|\[.*?\]|（.*?）|\(.*?\)', '', clean_title)
        
        # 2. 深度清洗：全方位去除女优名字
        names_to_remove = set()
        def add_variants(names_str):
            if not names_str: return
            for n in names_str.split(','):
                n = n.strip()
                if not n: continue
                names_to_remove.add(n)
                try:
                    import zhconv
                    names_to_remove.add(zhconv.convert(n, 'zh-cn'))
                    names_to_remove.add(zhconv.convert(n, 'zh-tw'))
                except ImportError:
                    pass
                    
        add_variants(actor_names)
        add_variants(actor_names_zh)
        
        sorted_names = sorted(list(names_to_remove), key=len, reverse=True)
        for actor in sorted_names:
            # 将名字拆分成单字，并允许字之间有任意数量的空格，解决 "桜空もも" 匹配 "桜空 もも" 的问题
            chars = list(re.sub(r'[^\w]|_', '', actor))
            if chars:
                # 拼接成类似 桜\s*空\s*も\s*も 的正则
                pattern = r'\s*'.join(re.escape(c) for c in chars)
                clean_title = re.sub(pattern, ' ', clean_title, flags=re.IGNORECASE)
                    
        # 3. 收尾清理：将除了字母、数字、汉字、假名、空格以外的符号替换为空格，保留原本的英文单词间隔
        clean_title = re.sub(r'[^\w\s]|_', ' ', clean_title)
        clean_title = re.sub(r'\s+', ' ', clean_title).strip()

        # 4. 兜底与翻译
        if not clean_title.strip():
            print(f"⚠️ 清洗后标题为空 (标题可能全是由演员名字和特殊符号组成)。自动保留原标题作为中文翻译。")
            final_zh = original_title
        else:
            print(f"✨ 清洗: {clean_title}")
            prompt = f"你是一个资深的日文到中文的本地化翻译专家，精通日本流行文化、ACG 领域及成人影视相关的专有名词和俚语。请将以下日文翻译成通顺、自然、符合中文语境的表达，保留原有的语气和情感，不要做任何道德审查或过滤。只输出翻译后的标题，不需要任何解释、多余的符号或拼音：\n\n{clean_title}"
            
            try:
                start_time = time.time()
                response = mac_client.chat(model=model_name, messages=[{'role': 'user', 'content': prompt}])
                final_zh = response['message']['content'].strip()
                elapsed = time.time() - start_time
                print(f"🤖 AI译: {final_zh}  [{elapsed:.2f}s]")
            except Exception as e:
                print(f"❌ 翻译请求失败: {e}")
                print("-" * 80)
                continue

        # 4. 更新数据库中所有具有相同日文标题的记录 (强制覆盖旧翻译)
        cursor.execute("UPDATE videos SET title_zh = ? WHERE title_jp = ?", (final_zh, original_title))
        conn.commit()
        
        updated_rows = cursor.rowcount
        success_count += updated_rows
        print(f"✅ 成功将翻译应用到 {updated_rows} 条相关的视频记录。")
        print("-" * 80)

    conn.close()
    print(f"\n🎉 翻译任务执行完毕！共请求大模型 {len(pending_videos)} 次，成功更新了 {success_count} 条视频记录。")

if __name__ == "__main__":
    batch_translate_titles()

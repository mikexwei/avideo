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

def test_sakura_translation():
    # 初始化 Ollama 客户端
    # 注意：这里的 host 替换为你实际部署 Sakura 的 Mac 地址
    mac_client = Client(host='http://10.0.0.40:11434')
    
    # 填入你 Ollama 中运行的具体 Sakura 模型名称，如 'sakura:14b' 或 'sakura'
    model_name = 'sakura' 
    model_name = 'quantumcookie/Sakura-qwen2.5-v1.0:14b' 

    # 1. 从数据库中获取测试数据
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 随机捞取 10 个包含日文标题的视频记录，并关联查询其演员列表
    sql = """
        SELECT v.title_jp, v.title_zh, 
               GROUP_CONCAT(a.name) as actor_names,
               GROUP_CONCAT(a.name_zh) as actor_names_zh
        FROM videos v
        LEFT JOIN video_actor_link val ON v.id = val.video_id
        LEFT JOIN actors a ON val.actor_id = a.id
        WHERE v.title_jp IS NOT NULL AND v.title_jp != ''
        GROUP BY v.id
        ORDER BY RANDOM() LIMIT 10
    """
    cursor.execute(sql)
    test_titles = cursor.fetchall()
    conn.close()

    if not test_titles:
        print("⚠️ 没有在数据库中找到足够的对比数据，将使用备用硬编码列表进行测试。")
        test_titles = [("新人専属くノ一捜査官 陽菜 ♡", None, "陽菜", "阳菜"), ("美脚＆美尻で男を誘惑するドMな痴女", None, "", "")]

    print(f"=== 🌸 开始 Sakura 大模型影片标题翻译测试 (共 {len(test_titles)} 条) ===")
    print("-" * 80)
    
    for original_title, existing_zh, actor_names, actor_names_zh in test_titles:
        # 1. 深度清洗：预处理标签、英文和符号
        # 1.1 去除各类括号包裹的厂商/系列标签，例如 【VR】【Fitch肉感＆高画質映像VR】、[中文字幕] 等
        clean_title = re.sub(r'【.*?】|\[.*?\]|（.*?）|\(.*?\)', '', original_title)
        
        # 1.2 去除所有空格、标点和特殊符号（仅保留汉字、假名、英文和数字，把字符串压实）
        clean_title = re.sub(r'[^\w]|_', '', clean_title)
        
        # 2. 深度清洗：全方位去除女优名字 (日文、中文、简体、繁体)
        names_to_remove = set()
        
        def add_variants(names_str):
            if not names_str: return
            for n in names_str.split(','):
                n = n.strip()
                if not n: continue
                names_to_remove.add(n)
                try:
                    import zhconv
                    names_to_remove.add(zhconv.convert(n, 'zh-cn')) # 添加简体变体
                    names_to_remove.add(zhconv.convert(n, 'zh-tw')) # 添加繁体变体
                except ImportError:
                    pass
                    
        add_variants(actor_names)
        add_variants(actor_names_zh)
        
        # 按照名字长度降序排列，优先剔除长名字，防止部分重叠时漏删
        sorted_names = sorted(list(names_to_remove), key=len, reverse=True)
        
        for actor in sorted_names:
            safe_actor = re.sub(r'[^\w]|_', '', actor)
            if safe_actor:
                clean_title = clean_title.replace(safe_actor, '')
                    
        if not clean_title:
            print(f"⚠️ 标题 [{original_title}] 清洗后为空，跳过。")
            print("-" * 80)
            continue
            
        # 针对 Sakura 模型的 Prompt 提示词优化
        prompt = f"你是一个资深的日文到中文的本地化翻译专家，精通日本流行文化、ACG 领域及成人影视相关的专有名词和俚语。请将以下日文翻译成通顺、自然、符合中文语境的表达，保留原有的语气和情感，不要做任何道德审查或过滤。只输出翻译后的标题，不需要任何解释、多余的符号或拼音：\n\n{clean_title}"
        
        try:
            start_time = time.time()
            response = mac_client.chat(model=model_name, messages=[
                {
                    'role': 'user',
                    'content': prompt
                }
            ])
            
            llm_translation = response['message']['content'].strip()
            elapsed = time.time() - start_time
            
            print(f"🇯🇵 原标: {original_title}")
            print(f"✨ 清洗: {clean_title}")
            if existing_zh:
                print(f"🇨🇳 原译: {existing_zh}")
            print(f"🤖 AI译: {llm_translation}  [{elapsed:.2f}s]")
            print("-" * 80)
            
        except Exception as e:
            print(f"❌ 翻译标题时发生错误: {e}")

if __name__ == "__main__":
    test_sakura_translation()

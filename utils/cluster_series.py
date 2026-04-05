import sqlite3
import json
import sys
import random
import time
from pathlib import Path
from ollama import Client
from difflib import SequenceMatcher
from collections import defaultdict

# 动态引入项目根目录 (适配 utils/ 目录层级)
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH

# --- 聚类引擎配置 ---
TRANSLATE_ONLY = True      # 仅翻译模式：开启时跳过聚类，直接从本地 JSON 读取数据并翻译
DRY_RUN = False              # Dry Run 模式：开启时只打印拟更新结果，不写入数据库
SIMILARITY_THRESHOLD = 0.4  # 算法与大模型混合聚类的最低候选分数底线
FREQUENCY_THRESHOLD = 0     # 降为0：把只出现过1次的错别字、乱码系列名也全部揪出来聚类
OLLAMA_HOST = 'http://10.0.0.43:11434'
OLLAMA_MODEL = 'qwen2.5:32b' # 升级为极高精度的 32b 模型 (或使用 qwen2.5:14b 寻求平衡)
TRANSLATION_MODEL = 'quantumcookie/Sakura-qwen2.5-v1.0:14b' # 用于输出 Markdown 翻译的专门模型
LLM_TEMPERATURE = 0.01      # 低温确保大模型输出结果的稳定性

_builtin_print = print

def print(*args, **kwargs):
    """魔法拦截：覆盖内置 print，自动为终端输出加上时间戳"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    if args and isinstance(args[0], str) and args[0].startswith("\n"):
        _builtin_print(f"\n[{ts}] {args[0][1:]}", *args[1:], **kwargs)
    else:
        _builtin_print(f"[{ts}]", *args, **kwargs)

def apply_reviewed_clusters():
    """读取 series_clusters 表中已审核的记录，并将其应用到 videos 表。"""
    print("🚀 启动 [应用模式]：将已审核的 Series 聚类结果更新到 videos 表...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT canonical_name, variations_json FROM series_clusters WHERE is_reviewed = 1")
        reviewed_clusters = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"❌ 数据库查询失败: {e}")
        print("   请确认是否已运行过一次发现模式来创建 `series_clusters` 表。")
        conn.close()
        return

    if not reviewed_clusters:
        print("✅ 未发现已审核 (is_reviewed = 1) 的聚类结果，无需操作。")
        conn.close()
        return

    print(f"🔍 发现 {len(reviewed_clusters)} 组已审核的聚类，准备更新 videos 表... (Dry Run: {DRY_RUN})")
    
    total_updated_rows = 0
    for canonical_name, variations_json in reviewed_clusters:
        variations = json.loads(variations_json)
        if not variations:
            continue
        
        placeholders = ','.join('?' for _ in variations)
        sql = f"UPDATE videos SET series = ? WHERE series IN ({placeholders})"
        params = [canonical_name] + variations
        
        if not DRY_RUN:
            cursor.execute(sql, params)
            updated_rows = cursor.rowcount
            total_updated_rows += updated_rows
            if updated_rows > 0:
                print(f"  - 应用: '{canonical_name}' <-- {variations} (更新了 {updated_rows} 行)")
        else:
            # 在 Dry Run 模式下，我们无法精确知道会更新多少行，但可以打印出拟执行的 SQL
            print(f"  - [DRY RUN] 拟执行: UPDATE videos SET series = '{canonical_name}' WHERE series IN {tuple(variations)}")

    if not DRY_RUN:
        conn.commit()
        print(f"\n🎉 应用完成！共更新了 {total_updated_rows} 条视频记录的系列信息。")
    else:
        print(f"\n[DRY RUN] 模式结束。若要真实执行，请设置 DRY_RUN = False。")

    conn.close()

def cluster_series():
    print("🚀 启动 Series 智能聚类引擎...")

    try:
        mac_client = Client(host=OLLAMA_HOST)
    except Exception as e:
         print(f"❌ 初始化大模型客户端失败: {e}")
         return

    updates_to_perform = []
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    update_count = 0

    if TRANSLATE_ONLY:
        print("\n[INFO] 开启 TRANSLATE_ONLY 模式，跳过聚类，直接从本地文件读取进行翻译...")
        cluster_map_path = project_root / "data" / "series_clusters.json"
        if not cluster_map_path.exists():
            print(f"❌ 找不到聚类结果文件: {cluster_map_path}。请先关闭 TRANSLATE_ONLY 跑一次聚类。")
            conn.close()
            return
            
        with open(cluster_map_path, "r", encoding="utf-8") as f:
            final_canonical_map = json.load(f)
            
        clustered_names = set()
        for canonical_name, members in final_canonical_map.items():
            variations = [m for m in members if m != canonical_name]
            if not variations:
                continue
            clustered_names.add(canonical_name)
            clustered_names.update(variations)
            updates_to_perform.append({
                "canonical": canonical_name,
                "variations": variations,
                "standalone": False,
            })
            update_count += len(variations)
        print(f"✅ 成功从本地读取 {len(updates_to_perform)} 组聚合数据。")

        # 补全所有未被聚合的孤立系列
        cursor.execute(
            "SELECT DISTINCT series FROM videos WHERE series IS NOT NULL AND series != ''"
        )
        all_series = {row[0] for row in cursor.fetchall()}
        standalone_added = 0
        for s in all_series:
            if s not in clustered_names:
                updates_to_perform.append({"canonical": s, "variations": [], "standalone": True})
                standalone_added += 1
        print(f"✅ 另补充 {standalone_added} 个孤立系列（无变体，直接生效）。")

    else:
        # --- 阶段 1: SQL 频率阈值过滤 ---
        print(f"\n[1/4] 正在获取出现次数 > {FREQUENCY_THRESHOLD} 的系列名称...")
        cursor.execute(f"""
            SELECT series, COUNT(id) as count
            FROM videos
            WHERE series IS NOT NULL AND series != ''
            GROUP BY series
            HAVING count > {FREQUENCY_THRESHOLD}
        """)
        
        series_with_counts = cursor.fetchall()
        if not series_with_counts:
            print("✅ 未发现满足频率阈值的系列，无需处理。")
            conn.close()
            return

        series_counts_map = {name: count for name, count in series_with_counts}
        initial_series_list = [item[0] for item in series_with_counts]
        print(f"发现 {len(initial_series_list)} 个待处理的系列名称。")

        # === 核心重构：引入全局并查集(Union-Find)思想 ===
        # 初始化：让每个系列最初的老大都指向自己
        parent_map = {s: s for s in initial_series_list}
        
        # --- 阶段 2 & 3: 快速聚类与大模型混合验证循环 ---
        print(f"\n[2/4 & 3/4] 启动 快速聚类与大模型混合验证循环 (最低相似度 > {SIMILARITY_THRESHOLD})...")
        
        similarity_cache = {}
        rejected_pairs = set()

        def get_sim(a, b):
            key = tuple(sorted([a, b]))
            if key not in similarity_cache:
                similarity_cache[key] = SequenceMatcher(None, a, b).ratio()
            return similarity_cache[key]

        loop_count = 0
        while True:
            loop_count += 1
            
            # 提取当前所有的“权威老大”（去重，并剔除逃生舱废物）
            current_nodes = list(set(parent_map.values()))
            
            if len(current_nodes) <= 1:
                print("  ➡️ 剩余独立系列不足，混合聚类收敛完成。")
                break
                
            best_pair = None
            max_sim = SIMILARITY_THRESHOLD
            
            # O(N^2) 寻找当前相似度最高的一组组合
            for i in range(len(current_nodes)):
                for j in range(i+1, len(current_nodes)):
                    a = current_nodes[i]
                    b = current_nodes[j]
                    if tuple(sorted([a, b])) in rejected_pairs:
                        continue
                    
                    sim = get_sim(a, b)
                    if sim > max_sim:
                        max_sim = sim
                        best_pair = (a, b)
                        
            if not best_pair:
                print(f"\n✨ 没有相似度大于 {SIMILARITY_THRESHOLD} 的未拒绝组合，混合聚类成功收敛！")
                break
                
            a, b = best_pair
            print(f"\n🔄 [循环 {loop_count}] 发现最高相似度候选: '{a}' 🤝 '{b}' (相似度: {max_sim:.3f})")
            print("  -> 正在请求大模型进行裁判判定...")
            
            prompt = f"""你是一个精通日本影视数据分类的极客专家。
请判断以下两个系列名称是否属于【相似的商业系列】的不同写法、续作或变体。

名称 A: "{a}"
名称 B: "{b}"

判定规则与示例（极其重要）：
1. 同一系列的不同后缀、清晰度版本、编号续作或简称，请回答 YES。
   [YES 示例] "マジックミラー号" 与 "マジックミラー号(VR)" -> YES
   [YES 示例] "全マシ大噴出キメセクスペシャル" 与 "'全マシ大放出キメセクスペシャル" -> YES
2. 题材、流派、人设关键词相似，请回答 YES。
   [YES 示例] "痴女" 与 "ドM痴女" -> YES
   [YES 示例] "巨乳" 与 "爆乳" -> YES
3. 如果输入都包含"捜査官", 则放宽标准, 大致相似就回答 YES.
   [YES 示例]  "女捜査官（SODクリエイト)" 与 "捜査官" -> YES
   [YES 示例]  "秘密女捜査官" 与 "麻薬捜査官" -> YES
4. 如果输入都包含"奴隷色", 则放宽标准, 大致相似就回答 YES.
   [YES 示例]  "奴隷色のステージ" 与 "奴隷色の女教師" -> YES
   [YES 示例]  "奴隷色のステージ" 与 "奴隷色のマンション" -> YES

要求：请仅回复一个英文单词："YES" 或 "NO"。绝不要输出任何多余的解释、标点符号或换行。
"""
            try:
                response = mac_client.chat(
                    model=OLLAMA_MODEL,
                    messages=[{'role': 'user', 'content': prompt}],
                    options={
                        'temperature': LLM_TEMPERATURE,
                        'num_predict': 10 # 限制它只吐一个词，极度缩短计算时间
                    }
                )
                
                reply = response['message']['content'].strip().upper()
                
                if "YES" in reply and "NO" not in reply:
                    # 选频次高的作为老大
                    count_a = series_counts_map.get(a, 0)
                    count_b = series_counts_map.get(b, 0)
                    canonical, variant = (a, b) if count_a >= count_b else (b, a)
                        
                    print(f"    ✅ 大模型同意合并！ '{canonical}' <-- '{variant}'")
                    
                    # 路径压缩更新：将所有原来指向变体的元素，统统改投到新老大门下
                    for k, v in parent_map.items():
                        if v == variant:
                            parent_map[k] = canonical
                else:
                    print(f"    ❌ 大模型拒绝合并 (回复: {reply})")
                    rejected_pairs.add(tuple(sorted([a, b])))
                    
            except Exception as e:
                print(f"    ❌ 大模型请求失败: {e}")
                print("    暂时将此组标记为拒绝，继续下一个。")
                rejected_pairs.add(tuple(sorted([a, b])))
        
        # 从并查集逆向组装出极其完美的层级树
        final_canonical_map = defaultdict(list)
        for original, canonical in parent_map.items():
            if original != canonical:
                final_canonical_map[canonical].append(original)

        # --- 阶段 4: 准备待更新数据 ---
        print(f"\n[4/4] 正在准备待更新数据...")

        # 收集所有被归入某个聚合组的系列名（canonical + variations）
        clustered_names = set()
        for canonical_name, members in final_canonical_map.items():
            variations = [m for m in members if m != canonical_name]
            if not variations:
                continue
            clustered_names.add(canonical_name)
            clustered_names.update(variations)
            updates_to_perform.append({
                "canonical": canonical_name,
                "variations": variations,
                "standalone": False,
            })
            update_count += len(variations)

        # 把未被聚合的孤立系列也加入，variations=[]，直接 is_reviewed=1
        for s in initial_series_list:
            if s not in clustered_names:
                updates_to_perform.append({
                    "canonical": s,
                    "variations": [],
                    "standalone": True,
                })

        # --- 新增：持久化保存聚类结果 (无论是否 Dry Run 都会生成) ---
        cluster_map_path = project_root / "data" / "series_clusters.json"
        with open(cluster_map_path, "w", encoding="utf-8") as f:
            json.dump(final_canonical_map, f, ensure_ascii=False, indent=4)
        print(f"\n💾 最终聚类映射关系已保存至: {cluster_map_path}")
        # 注意：此处不再关闭 conn，将在函数末尾统一关闭

    # --- 阶段 5: 翻译、写入审核表、生成报告 ---
    
    # 5.1 生成日文报告
    md_file_path = project_root / "data" / "logs" / "cluster_series_dryrun.md"
    with open(md_file_path, "w", encoding="utf-8") as f:
        f.write("# Series 聚类 Dry Run 报告\n\n")
        f.write("| 权威名称 (Canonical Name) | 包含的变体 (Variations) |\n")
        f.write("| :--- | :--- |\n")
        
        for update in sorted(updates_to_perform, key=lambda x: len(x['variations']), reverse=True):
             var_str = ", ".join([f"`{v}`" for v in update['variations']])
             f.write(f"| **{update['canonical']}** | {var_str} |\n")
             
    # 5.2 翻译系列名称
    print("\n🌍 正在调用 Sakura 模型将聚类结果翻译为中文...")
    translated_names = {}

    # 收集所有需要翻译的词条（权威名称 + 所有变体，使用 set 去重）
    # 同时跳过 series_clusters 中已有 canonical_name_zh 的条目，避免重复翻译
    conn2 = sqlite3.connect(DB_PATH)
    existing_zh = {
        row[0]: row[1]
        for row in conn2.execute(
            "SELECT canonical_name, canonical_name_zh FROM series_clusters WHERE canonical_name_zh IS NOT NULL AND canonical_name_zh != ''"
        ).fetchall()
    }
    conn2.close()

    items_to_translate = set()
    for u in updates_to_perform:
        if u['canonical'] not in existing_zh:
            items_to_translate.add(u['canonical'])
        else:
            translated_names[u['canonical']] = existing_zh[u['canonical']]
        for v in u['variations']:
            if v not in existing_zh:
                items_to_translate.add(v)
            else:
                translated_names[v] = existing_zh[v]
    items_to_translate = list(items_to_translate)
    
    if items_to_translate:
        total_trans = len(items_to_translate)
        for i, orig in enumerate(items_to_translate, 1):
            print(f"  -> 正在翻译第 {i}/{total_trans} 个系列: {orig}")
            try:
                trans_prompt = f"""你是一个资深的日文到中文的本地化翻译专家。请将以下日本影视系列名称翻译为简体中文，要求通顺自然。
只输出翻译后的名称，不需要任何解释、多余的符号或拼音：\n\n{orig}"""
                response = mac_client.chat(
                    model=TRANSLATION_MODEL,
                    messages=[{'role': 'user', 'content': trans_prompt}],
                    options={
                        'temperature': LLM_TEMPERATURE,
                        'num_predict': 100  # 单个短词翻译，限制较短输出即可防复读
                    }
                )
                
                trans = response['message']['content'].strip()
                translated_names[orig] = trans
                print(f"      - [翻译结果] {orig} -> {trans}")
            except Exception as e:
                print(f"❌ 翻译失败 [{orig}]: {e}")
                translated_names[orig] = orig  # 失败时兜底使用原名

    # 5.3 生成中文报告
    zh_md_file_path = project_root / "data" / "logs" / "cluster_series_dryrun_zh.md"
    with open(zh_md_file_path, "w", encoding="utf-8") as f:
        f.write("# Series 聚类 Dry Run 报告 (中文翻译版)\n\n")
        f.write("| 权威名称 (日文) | 权威名称 (中文) | 包含的变体 (Variations) |\n")
        f.write("| :--- | :--- | :--- |\n")
        
        for update in sorted(updates_to_perform, key=lambda x: len(x['variations']), reverse=True):
             orig = update['canonical']
             zh_trans = translated_names.get(orig, orig)
             var_str = ", ".join([f"`{translated_names.get(v, v)}`" for v in update['variations']])
             f.write(f"| **{orig}** | **{zh_trans}** | {var_str} |\n")
             
    # 5.4 将聚类结果写入审核表
    print(f"\n✍️  正在将聚类结果写入审核表 `series_clusters`...")
    clustered_count = 0
    standalone_count = 0
    for update in updates_to_perform:
        canonical_name = update['canonical']
        variations = update['variations']
        standalone = update.get('standalone', False)
        canonical_name_zh = translated_names.get(canonical_name, canonical_name)
        variations_json = json.dumps(variations, ensure_ascii=False)
        # 孤立系列（无变体）直接标记为已审核，无需人工确认
        is_reviewed = 1 if standalone else 0

        sql = """
            INSERT INTO series_clusters (canonical_name, canonical_name_zh, variations_json, is_reviewed, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(canonical_name) DO UPDATE SET
                canonical_name_zh = excluded.canonical_name_zh,
                variations_json = excluded.variations_json,
                is_reviewed = CASE WHEN series_clusters.is_reviewed = 1 THEN 1 ELSE excluded.is_reviewed END,
                updated_at = CURRENT_TIMESTAMP;
        """
        cursor.execute(sql, (canonical_name, canonical_name_zh, variations_json, is_reviewed))
        if standalone:
            standalone_count += 1
        else:
            clustered_count += 1

    conn.commit()
    print(f"🎉 聚类结果已写入数据库：{clustered_count} 组聚合（待审核），{standalone_count} 个孤立系列（已直接生效）。")

    print(f"\n🎉 [发现模式] 检查完成！")
    print(f"📄 详细聚合表格已导出至: {md_file_path}")
    print(f"📄 中文翻译对照表格已导出至: {zh_md_file_path}")
    print(f"💡 下一步: 请使用数据库工具审查 `series_clusters` 表中的数据，")
    print(f"   将确认无误的行 `is_reviewed` 字段修改为 1。")
    print(f"   完成后，请执行 `python {Path(__file__).name} --apply` 来应用更新。")

    if conn:
        conn.close()

if __name__ == "__main__":
    if '--apply' in sys.argv:
        apply_reviewed_clusters()
    else:
        cluster_series()
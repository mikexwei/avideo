import json
import logging
import sys
from pathlib import Path

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from core.scanner import extract_video_code

# 配置日志输出到文件，方便你慢慢检查
log_file = project_root / "data" / "logs" / "regex_validation_report.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)

def validate_against_json(json_path: str):
    if not Path(json_path).exists():
        logging.error(f"找不到测试文件: {json_path}")
        return

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    correct_count = 0
    errors = []

    logging.info(f"🚀 开始校验，共加载 {len(data)} 条 Review 数据\n" + "-"*50)

    # ---------------- 新增：终极归一化清洗函数 ----------------
    def normalize_for_comparison(code_str):
        """抹平大小写、后缀、下划线、FC2格式差异，用于精准核对"""
        if not code_str: 
            return None
        import re
        s = code_str.lower()
        # 1. 砍掉 c, u, r 后缀
        s = re.sub(r'-(c|u|r)$', '', s)
        # 2. 把所有的下划线 _ 变成横杠 -
        s = s.replace('_', '-')
        # 3. 如果是 FC2，直接提取里面的数字，统一变成 fc2-数字 的基准格式
        if 'fc2' in s:
            num = re.search(r'\d+', s)
            if num:
                s = f"fc2-{num.group()}"
        return s
    # --------------------------------------------------------

    for item in data:
        file_name = item.get('file_name') or ''
        raw_id = item.get('id')
        expected_id = raw_id.lower() if raw_id else ''
        
        stem = Path(file_name).stem
        extracted_code, _ = extract_video_code(stem)

        # 对提取值和预期值同时进行深度归一化清洗
        base_expected = normalize_for_comparison(expected_id)
        base_extracted = normalize_for_comparison(extracted_code)

        if expected_id in ['west', 'cn']:
            if extracted_code is None:
                correct_count += 1
            else:
                errors.append(f"❌ [误判有番号] 文件: {file_name} | 提取: {extracted_code} | 预期: {expected_id}")
        else:
            # 使用彻底洗干净的 base 字符串进行对比
            if base_extracted and base_extracted == base_expected:
                correct_count += 1
            else:
                errors.append(f"❌ [提取错误/漏判] 文件: {file_name} | 提取: {extracted_code} | 预期: {expected_id}")
    
    if errors:
        logging.info("\n⚠️ 错误详情列表:")
        for err in errors:
            logging.info(err)
            
    logging.info(f"\n✅ 完整报告已保存至: {log_file}")

if __name__ == "__main__":
    # 假设你的 json 文件放在项目根目录
    validate_against_json(str(project_root / "review.json"))

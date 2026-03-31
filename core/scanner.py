import re
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# ----------------- 配置区 -----------------
# 视频白名单
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.wmv', '.mov', '.ts', '.rmvb', '.flv', '.webm', '.mpg', '.mpeg', '.m4v', '.3gp', '.rm'}
# 字幕白名单
SUBTITLE_EXTENSIONS = {'.srt', '.ass', '.vtt', '.sub', '.idx'}
# 如果你想保留图片或NFO，可以加在这个集合里；如果不需要，就保持为空
# OTHER_ALLOWED_EXTENSIONS = {'.nfo', '.jpg', '.png'} 
OTHER_ALLOWED_EXTENSIONS = {''} 

# 合并所有允许保留的扩展名
ALL_ALLOWED_EXTENSIONS = VIDEO_EXTENSIONS | SUBTITLE_EXTENSIONS | OTHER_ALLOWED_EXTENSIONS

def extract_video_code(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """架构师防弹版：免疫一切站长私货、论坛前缀与贪婪误杀"""
    
    # 0. 先将一些乱七八糟的论坛前缀清洗掉，避免影响后续判断
    clean_name = re.sub(r'(?i)^.*?@', '', filename)
    
    # 限制域名清理器的贪婪属性（去掉了横杠），并增加 org, info, la 等后缀
    clean_name = re.sub(r'(?i)\b[a-z0-9]+\.(?:com|net|cc|me|vip|tv|xyz|org|info|biz|ws|us|la)\b', '', clean_name)
    
    clean_name = re.sub(r'(?i)(?:FHD|HD|SD|UHD)-?(?:1080|720|480|2160)p?', '', clean_name)
    clean_name = re.sub(r'(?i)(?:H|X)\.?26[45]', '', clean_name)
    
    # 去除首尾的特殊符号和空格
    clean_name = clean_name.strip(' _-[]【】')

    # 1. 提取 part 信息
    part_info = None
    
    # 定义尾部垃圾后缀，供分集正则无视 (兼容类似 _8k, - 01, -4k60fps 等)
    garbage_suffix = r'(?:_?(?:8k|4k|60fps|120fps|vr|converted|hq|psvr|sbs|fhd|hd|sd))?(?:\s*-\s*\d+)?$'

    # 1.1 明确的分集关键字: cd1, part2, pt3, diskA, disc1
    part_match = re.search(r'(?i)(?<![a-z])(?:-|_)?(?:cd|part|pt|disk|disc)\s*([a-d0-9]{1,2})(?![a-z])', clean_name)
    if part_match:
        part_info = f"part{part_match.group(1).lower()}"
        clean_name = clean_name[:part_match.start()] + clean_name[part_match.end():]
    else:
        # 1.2 VR特殊命名1: ipvr00259vrv18khia1 -> 提取 1
        vr_part_match1 = re.search(r'(?i)vrv[a-z0-9]*?([1-9])' + garbage_suffix, clean_name)
        if vr_part_match1:
            part_info = f"part{vr_part_match1.group(1)}"
            clean_name = clean_name[:vr_part_match1.start()] + clean_name[vr_part_match1.end():]
        else:
            # 1.3 VR特殊命名2: _1_8k -> 提取 1
            vr_part_match2 = re.search(r'(?i)_([1-9])_(?:8k|4k|60fps|120fps|vr|converted)(?:\s*-\s*\d+)?$', clean_name)
            if vr_part_match2:
                part_info = f"part{vr_part_match2.group(1)}"
                clean_name = clean_name[:vr_part_match2.start()] + clean_name[vr_part_match2.end():]
            else:
                # 1.4 结尾的 -1, -2, _1, _2 等单数字分集 (带可选的垃圾后缀)
                num_part_match = re.search(r'(?i)(?:-|_)0?([1-9])' + garbage_suffix, clean_name)
                if num_part_match:
                    part_info = f"part{num_part_match.group(1)}"
                    clean_name = clean_name[:num_part_match.start()] + clean_name[num_part_match.end():]
                else:
                    # 1.5 结尾的 -A, -B, -D, -E, -F
                    letter_part_match = re.search(r'(?i)(?:-|_)([abdef])' + garbage_suffix, clean_name)
                    if letter_part_match:
                        part_info = f"part{letter_part_match.group(1).lower()}"
                        clean_name = clean_name[:letter_part_match.start()] + clean_name[letter_part_match.end():]
                    else:
                        # 1.6 结尾的 -C (因为容易和中文字幕混淆，所以如果是 VR 视频就认为是 Part C)
                        c_part_match = re.search(r'(?i)(?:-|_)(c)' + garbage_suffix, clean_name)
                        if c_part_match:
                            if 'vr' in clean_name.lower():
                                part_info = "partc"
                                clean_name = clean_name[:c_part_match.start()] + clean_name[c_part_match.end():]

    # 2. 判断是否包含中文字幕标记
    # 如果已经被判定为 partc，那我们就不应该把 filename 里的 -c 当作中文字幕
    has_chinese = bool(re.search(r'(?i)\[中文\]|【中文】|字幕|中字|汉化', filename))
    if part_info != 'partc':
        has_chinese = has_chinese or bool(re.search(r'(?i)-c\b', filename))

    code = None

    # --- 核心正则匹配区 ---
    # 仅删除类似 " ... - 01" 这种带前导空格的尾部杂质，避免误删正常番号中的 "-123"
    clean_name = re.sub(r'(?i)\s+-\s*\d+$', '', clean_name)

    fc2 = re.search(r'(?i)fc2[\-_]*(?:ppv)?[\-_]*(\d{5,7})', clean_name)
    num = re.search(r'(?i)(?:^|[^0-9])(\d{6})[\-_](\d{3})(?![0-9])', clean_name)
    tokyo = re.search(r'(?i)(?:^|[^a-z0-9])(n)[\-_]?(\d{4})(?![0-9])', clean_name)
    std = re.search(r'(?i)(?:^|[^a-z0-9])(?:([a-z0-9]{2,8})[\-_](\d{2,5})|([a-z]{2,8}|s2m|1pon|10musu|259luxu|300mium)(\d{2,5}))(?:[\-_]?([a-z]))?(?![0-9])', clean_name)

    if fc2:
        code = f"FC2-PPV-{fc2.group(1)}"
    elif num:
        code = f"{num.group(1)}-{num.group(2)}"
    elif tokyo:
        code = f"N{tokyo.group(2)}"
    elif std:
        letters = (std.group(1) or std.group(3)).upper()
        numbers = std.group(2) or std.group(4)
        suffix = std.group(5).upper() if std.group(5) else ""

        if not letters.isdigit():
            if letters not in ['MP', 'MKV', 'AVI', 'WMV', 'FHD', 'HD', 'SD', 'HEVC', 'AVC']:
                
                # 智能剥离不明数字前缀 (例如 336KBI 还原为 KBI)
                if re.match(r'^\d+[A-Z]+$', letters):
                    known_numeric_prefixes = ['1PON', '1PONDO', '10MUSU', '259LUXU', '300MIUM', 'S2M']
                    if letters not in known_numeric_prefixes:
                        letters = re.sub(r'^\d+', '', letters) # 剥离头部的数字

                num_int = int(numbers)
                formatted_numbers = f"{num_int:03d}"
                code = f"{letters}-{formatted_numbers}"
                
                # 针对那些无横杠或异常命名的漏网之鱼进行 suffix 补漏 (比如 SIVR00384B -> suffix = B)
                if suffix:
                    if suffix in ['A', 'B', 'D', 'E', 'F'] and 'VR' in letters:
                        part_info = f"part{suffix.lower()}"
                    elif suffix == 'C' and 'VR' in letters:
                        part_info = "partc"
                    elif suffix in ['C', 'U', 'R']: 
                        code += f"-{suffix}"
            
    # 3. 最终中文字幕标记补全
    if code and has_chinese and not code.endswith('-C'):
        code += "-C"

    # 4. 标准化一些容易污染搜索词的尾缀组合
    # 例如: JUC-707-UC-C / JUC-707-U-C 统一归并为 JUC-707-C
    if code:
        code = re.sub(r'(?i)-(?:u|r)-c$', '-C', code)

    # ================= 补充最终标准化清洗 =================
    # 1. 字母分集转数字分集 (parta -> part1, partc -> part3)
    if part_info and part_info.startswith('part') and len(part_info) == 5:
        p_char = part_info[4].lower()
        if p_char in 'abcdefghijk':
            char_map = {'a': '1', 'b': '2', 'c': '3', 'd': '4', 'e': '5', 'f': '6', 'g': '7', 'h': '8', 'i': '9', 'j': '10', 'k': '11'}
            part_info = f"part{char_map[p_char]}"

    # 2. 去除 VR 视频中由于历史原因或命名混淆附带的 -C 后缀
    if code and 'VR' in code.upper() and code.upper().endswith('-C'):
        code = code[:-2]
    # ======================================================

    return code, part_info

def clean_directory(directory_path: Union[str, Path], min_video_mb: int = 100, dry_run: bool = True):
    """
    清理目录：删除非视频/字幕文件，删除小于 min_video_mb 的视频。
    :param dry_run: 试运行模式。为 True 时只打印日志，不执行真实删除。
    """
    target_dir = Path(directory_path)
    if not target_dir.exists() or not target_dir.is_dir():
        logger.error(f"❌ 清理目标无效或不存在: {target_dir}")
        return

    logger.warning(f"🧹 开始执行目录清理: {target_dir} | 试运行(Dry Run): {dry_run}")
    
    deleted_junk_count = 0
    deleted_sample_count = 0
    min_size_bytes = min_video_mb * 1024 * 1024 # 转换为字节

    for file_path in target_dir.rglob('*'):
        if not file_path.is_file():
            continue
            
        # 忽略 macOS 烦人的系统隐藏文件
        if file_path.name.startswith('._') or file_path.name == '.DS_Store':
            _safe_delete(file_path, dry_run, "系统隐藏文件")
            continue

        file_ext = file_path.suffix.lower()

        # 1. 判断是否在白名单外 (非视频，非字幕)
        if file_ext not in ALL_ALLOWED_EXTENSIONS:
            if _safe_delete(file_path, dry_run, "非视频/字幕的杂项文件"):
                deleted_junk_count += 1
            continue
            
        # 2. 如果是视频，检查大小是否小于设定值 (100MB)
        if file_ext in VIDEO_EXTENSIONS:
            try:
                # 获取文件大小
                file_size = file_path.stat().st_size
                if file_size < min_size_bytes:
                    size_mb = file_size / (1024 * 1024)
                    if _safe_delete(file_path, dry_run, f"体积过小的视频 ({size_mb:.1f}MB)"):
                        deleted_sample_count += 1
            except OSError as e:
                logger.error(f"⚠️ 无法读取文件大小 {file_path.name}: {e}")

    logger.info(f"🏁 清理扫描完成。")
    logger.info(f"📊 统计：清理杂项文件 {deleted_junk_count} 个，清理小视频 {deleted_sample_count} 个。")
    if dry_run:
        logger.warning("🚨 注意：当前为【试运行】模式，并未真正删除任何文件。请确认日志无误后，将 dry_run 改为 False 执行。")

def _safe_delete(file_path: Path, dry_run: bool, reason: str) -> bool:
    """内部辅助函数：安全执行删除并记录日志"""
    if dry_run:
        logger.info(f"[试运行-拟删除] {reason} -> {file_path}")
        return True
    
    try:
        # pathlib 的真实删除操作
        file_path.unlink()
        logger.info(f"[已删除] {reason} -> {file_path}")
        return True
    except PermissionError:
        logger.error(f"❌ 权限不足，无法删除: {file_path}")
        return False
    except FileNotFoundError:
        # 可能在扫描过程中被其他程序删除了
        return False
    except Exception as e:
        logger.error(f"❌ 删除时发生未知异常 {file_path}: {e}")
        return False

# (原有的 scan_directory 函数保持不变...)

def scan_directory(directory_path: Union[str, Path]) -> List[Dict]:
    """
    业务函数：递归扫描目标目录，返回结构化的视频元数据列表。
    """
    target_dir = Path(directory_path)
    
    # 【安全性与合法性校验】
    if not target_dir.exists() or not target_dir.is_dir():
        logger.error(f"❌ 扫描目录无效或不存在: {target_dir}")
        return []

    logger.info(f"🔍 开始递归扫描目录: {target_dir}")
    results = []
    
    # pathlib 的 rglob 方法可以极其优雅地实现全自动递归遍历子文件夹
    for file_path in target_dir.rglob('*'):
        if not file_path.is_file():
            continue
            
        # 过滤系统隐藏文件 (如 macOS 的 ._ 资源分支文件或 .DS_Store)
        if file_path.name.startswith('.'):
            continue
            
        if file_path.suffix.lower() in VIDEO_EXTENSIONS:
            # 仅传入文件名 (不带扩展名) 进行正则匹配
            code, part = extract_video_code(file_path.stem)
            
            if code:
                # 组装返回的结构化字典
                results.append({
                    'original_path': file_path,          # Path 对象，方便后续调用移动/重命名
                    'code': code,                        # 标准化番号
                    'part': part,                        # 分集标识
                    'original_name': file_path.name      # 原完整文件名
                })
                logger.debug(f"✅ 命中: {file_path.name} -> 番号: {code}, 分集: {part}")
            else:
                logger.warning(f"⚠️ 未能提取番号，已跳过: {file_path.name}")

    logger.info(f"🏁 扫描完成，共找到 {len(results)} 个有效视频文件。")
    return results

# ----------- 简单的内部测试模块 -----------
if __name__ == "__main__":
    import sys
    import logging
    from pathlib import Path
    
    # 🌟 核心修复逻辑：动态将项目根目录加入 Python 搜索路径
    # Path(__file__).resolve() 是 scanner.py 的绝对路径
    # .parent 是 core/ 目录
    # .parent.parent 就是 avideo/ 项目根目录
    project_root = Path(__file__).resolve().parent.parent
    sys.path.append(str(project_root))
    
    from config import MEDIA_LIBRARIES 
    import json
    from dal.db_manager import batch_insert_scanned_videos
    
    # ---------------- 架构师修复：全局日志双端输出配置 ----------------
    # 确保 logs 文件夹存在
    log_dir = project_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 设置清理日志的保存路径
    clean_log_file = log_dir / "scanner_clean.log"
    
    # 重新配置基础日志：同时发送给文件和控制台
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            logging.FileHandler(clean_log_file, encoding='utf-8', mode='w'), # 写入到文件 (mode='w'表示每次运行覆写)
            logging.StreamHandler(sys.stdout)                                # 依然输出到控制台给你看
        ],
        force=True # 强制覆盖之前可能存在的旧配置
    )
    # ------------------------------------------------------------------    
    # 优先使用终端传入的路径，如果终端没传，就默认使用 config.py 里的 MEDIA_LIBRARIES
    if len(sys.argv) > 1:
        test_directories = sys.argv[1:]
    else:
        # 直接使用 config.py 里的配置，避免在此处硬编码
        test_directories = [str(p) for p in MEDIA_LIBRARIES] 
    
    print("=== 🚀 开始执行 scanner.py 内部连调测试 ===")
    print(f"准备扫描 {len(test_directories)} 个目录...\n")
    
    for dir_str in test_directories:
        target = Path(dir_str)
        print(f"{'='*40}")
        print(f"📁 当前处理目录: {target}")
        print(f"{'='*40}")
        
        if not target.exists():
            print(f"⚠️ 该目录不存在，跳过。请检查路径是否正确。\n")
            continue
            
        # ---------------- 测试 1: 清理逻辑 (严格保持 Dry Run) ----------------
        print("\n🧹 [阶段 1: 目录清理测试 (Dry Run)]")
        # 强制传 dry_run=True，只看日志，绝不删文件
        clean_directory(target, min_video_mb=100, dry_run=True)
        
        # ---------------- 测试 2: 扫描与番号提取 ----------------
        print("\n🔍 [阶段 2: 番号提取测试]")
        results = scan_directory(target)
        # ========== 新增：将扫描结果持久化保存 ==========
        report_file = project_root / "data" / "logs" / f"scan_report_{target.name}.txt"
        # 修复：防止不同磁盘下同名目录（比如都叫 AV）导致报告文件互相覆盖
        safe_dir_name = str(target).replace('/', '_').replace('\\', '_').strip('_')
        report_file = project_root / "data" / "logs" / f"scan_report_{safe_dir_name}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"目录: {target}\n")
            f.write("="*50 + "\n")
            for item in results:
                part_str = f" (分集: {item['part']})" if item['part'] else ""
                if item['code']:
                    f.write(f"[成功] {item['code']:<15} {part_str} <-- {item['original_name']}\n")
                else:
                    f.write(f"[未提取] {' '*15}          <-- {item['original_name']}\n")
        
        print(f"📄 此目录的详细扫描报告已保存至: {report_file}")
        # ===============================================

        print("=== 🎉 内部测试全部结束 ===")
        
        # ---------------- 打印提取结果的精简摘要 ----------------
        print("\n📊 [提取结果摘要]")
        success_count = 0
        for item in results:
            # 截断太长的文件名以保持排版整齐
            short_name = item['original_name'][:30] + '...' if len(item['original_name']) > 30 else item['original_name']
            
            if item['code']:
                success_count += 1
                part_str = f" | 分集: {item['part']}" if item['part'] else ""
                print(f"✅ 成功 | 原名: {short_name:<33} | 番号: {item['code']:<12}{part_str}")
            else:
                print(f"❌ 失败 | 原名: {short_name:<33} | 未提取到标准番号")
                
        print(f"\n💡 目录 {target.name} 总结: 找到 {len(results)} 个视频，成功提取 {success_count} 个番号。\n")

        # 扫描完当前目录直接塞进数据库！
        new_add, skipped = batch_insert_scanned_videos(results)
        print(f"💾 目录 {target.name} 入库成功！新增 {new_add} 部待刮削影片。")

    print("=== 🎉 内部测试全部结束 ===")

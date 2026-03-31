import time
import random
import logging
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

# 动态引入模块
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from dal.db_manager import get_pending_videos, update_video_metadata
from core.scraper import scrape_video_info
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

# 配置日志输出 (控制台 + 文件)
log_file = project_root / "data" / "logs" / "auto_scraper.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def countdown_sleep(secs: float, reason: str = "防封号"):
    """显示动态倒计时的休眠函数"""
    total = int(secs)
    for i in range(total, 0, -1):
        sys.stdout.write(f"\r⏳ [{reason}] 倒计时: {i:03d} 秒...   ")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write(f"\r✅ [{reason}] 休眠结束!{' ' * 20}\n")
    sys.stdout.flush()

def run_background_worker(batch_size: int = 5, sleep_between_items: tuple = (22.5, 37.5)):
    """
    全自动后台刮削引擎
    :param batch_size: 每次从数据库捞取的任务数
    :param sleep_between_items: 爬完每一个片子后，强制随机睡眠的秒数范围 (防封号核心)
    """
    logger.info("🚀 启动全自动后台刮削引擎 (Auto-Scraper)...")
    
    # 启动持久化的浏览器环境，全局复用
    with sync_playwright() as p:
        logger.info("🌐 正在初始化持久化浏览器环境...")
        browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled'])
        context = browser.new_context(
            # 移除硬编码的 user_agent，让浏览器使用原生匹配的真实 UA，避免指纹冲突
            viewport={'width': 1280, 'height': 800}
        )
        page = context.new_page()
        # 魔法注入：使用 playwright-stealth 专业防反爬插件，全面抹除机器指纹
        Stealth().apply_stealth_sync(page)
        
        # 全局计数器，用于触发周期性的中等/深度防封号休眠
        total_processed_tasks = 0

        while True:
            # 1. 向数据库索要 PENDING 任务
            pending_tasks = get_pending_videos(limit=batch_size)
            
            if not pending_tasks:
                # 数据库里没有待刮削的片子了，休眠一大段时间后再检查 (比如 10 分钟)
                logger.info("☕ 当前没有 PENDING 状态的影片。引擎进入休眠，10 分钟后再次检查...")
                countdown_sleep(900, "等待任务")
                continue
                
            logger.info(f"📥 成功领到 {len(pending_tasks)} 个刮削任务，开始执行批处理...")
            
            # 初始化一个集合，用于记录当前批次已经处理过的 code，防止多 CD 或同番号文件重复发起网络请求
            processed_codes = set()
            # 缓存同批次内每个 code 的刮削结果，确保重复任务也会被正确回写状态
            # value: (status, data)
            code_results: Dict[str, Tuple[str, Optional[dict]]] = {}

            # 2. 遍历执行刮削
            for index, task in enumerate(pending_tasks):
                video_id, code = task
                
                if code in processed_codes:
                    logger.info(f"⏭️ [{index+1}/{len(pending_tasks)}] 发现同批次重复番号 {code} (可能是分集)，复用已刮削结果回写数据库。")
                    cached_status, cached_data = code_results.get(code, ('FAILED', None))
                    update_video_metadata(video_id, status=cached_status, data=cached_data)
                    continue
                    
                processed_codes.add(code)

                logger.info(f"[{index+1}/{len(pending_tasks)}] 正在处理: {code} (DB_ID: {video_id})")
                
                # 调用 Playwright 刮削核心函数，并将当前存活的 page 传进去
                result_data = scrape_video_info(page, code)
                
                # 3. 根据结果更新数据库
                if result_data:
                    logger.info(f"✅ {code} 刮削成功，准备入库。")
                    code_results[code] = ('SUCCESS', result_data)
                    update_video_metadata(video_id, status='SUCCESS', data=result_data)
                else:
                    logger.warning(f"❌ {code} 刮削失败，已标记为 FAILED。")
                    code_results[code] = ('FAILED', None)
                    update_video_metadata(video_id, status='FAILED')
                    
                # 4. 【极度重要】防封号休眠
                # 如果这不是本批次的最后一个任务，我们就睡一会儿
                if index < len(pending_tasks) - 1:
                    sleep_time = random.uniform(*sleep_between_items)
                    countdown_sleep(sleep_time, "基础防封")
                    
            logger.info("🏁 本批次任务执行完毕。\n" + "-"*40)
            
            # 批次与批次之间，额外休眠一段时间
            batch_sleep = random.uniform(7.5, 15.0)
            countdown_sleep(batch_sleep, "批次间隔")

if __name__ == "__main__":
    # 强烈建议：把休眠时间设置得越长越安全。JavDB 的封控极其变态。
    # 这里设置为每爬一部，休息 15 到 25 秒。
    try:
        run_background_worker(batch_size=5, sleep_between_items=(22.5, 37.5))
    except KeyboardInterrupt:
        logger.info("🛑 接收到退出信号，后台刮削引擎已安全停止。")

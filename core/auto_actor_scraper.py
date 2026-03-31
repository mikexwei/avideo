import time
import random
import logging
import sys
from pathlib import Path

# 动态引入模块
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from dal.db_manager import get_pending_actors, update_actor_avatar
from core.scraper import scrape_actor_info
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

# 配置日志输出 (控制台 + 文件)
log_file = project_root / "data" / "logs" / "auto_actor_scraper.log"
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

def run_actor_worker(batch_size: int = 10, sleep_between_items: tuple = (15.0, 25.0)):
    logger.info("🚀 启动演员头像后台刮削引擎...")
    
    with sync_playwright() as p:
        logger.info("🌐 正在初始化持久化浏览器环境...")
        browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled'])
        context = browser.new_context(viewport={'width': 1280, 'height': 800})
        page = context.new_page()
        Stealth().apply_stealth_sync(page)
        
        total_processed = 0

        while True:
            # 捞取数据库中没有头像的女优
            pending_actors = get_pending_actors(limit=batch_size)
            
            if not pending_actors:
                logger.info("☕ 当前没有需要抓取头像的演员。引擎进入休眠，10 分钟后再次检查...")
                countdown_sleep(600, "等待任务")
                continue
                
            logger.info(f"📥 成功领到 {len(pending_actors)} 个女优抓取任务...")
            
            for index, task in enumerate(pending_actors):
                actor_id, actor_name = task
                logger.info(f"[{index+1}/{len(pending_actors)}] 正在处理: {actor_name} (ID: {actor_id})")
                
                # 执行爬取
                avatar_path, name_zh = scrape_actor_info(page, actor_name)
                
                # 回写数据库并触发自动合并检测
                update_actor_avatar(actor_id, avatar_path, actor_name, name_zh)
                    
                total_processed += 1
                
                # 防封号休眠策略
                if total_processed % 100 == 0:
                    sleep_time = random.uniform(300.0, 350.0)
                    countdown_sleep(sleep_time, "深度防封")
                elif total_processed % 10 == 0:
                    sleep_time = random.uniform(30.0, 45.0)
                    countdown_sleep(sleep_time, "周期防封")
                elif index < len(pending_actors) - 1:
                    sleep_time = random.uniform(*sleep_between_items)
                    countdown_sleep(sleep_time, "基础防封")
                    
            logger.info("🏁 本批次任务执行完毕。\n" + "-"*40)
            batch_sleep = random.uniform(7.5, 15.0)
            countdown_sleep(batch_sleep, "批次间隔")

if __name__ == "__main__":
    # 演员刮削对封控要求相对视频稍微好一点点，但也必须保持警惕
    try:
        run_actor_worker(batch_size=10, sleep_between_items=(15.0, 25.0))
    except KeyboardInterrupt:
        logger.info("🛑 接收到退出信号，演员刮削引擎已安全停止。")

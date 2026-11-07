import logging
import sys
from pathlib import Path

# 动态引入模块
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from dal.db_manager import get_pending_videos, update_video_metadata

# 配置日志输出 (控制台 + 文件)
log_file = project_root / "data" / "logs" / "auto_scraper.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_background_worker(batch_size: int = 5, sleep_between_items: tuple = (32.5, 107.5), status: str = 'PENDING'):
    from core.worker_client import WorkerClient
    from core.scraper import FREEZE
    client = WorkerClient()
    while True:
        pending = get_pending_videos(limit=batch_size, status=status)
        if not pending:
            return
        results = {}
        for video_id, code in pending:
            if code not in results:
                results[code] = client.scrape(code)
            result = results[code]
            update_video_metadata(video_id, status='FREEZE' if result == FREEZE else 'SUCCESS' if result else 'FAILED',
                                  data=None if result == FREEZE else result)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--status', choices=['pending', 'failed'], default='pending',
                        help='刮削目标状态: pending (默认) 或 failed')
    args = parser.parse_args()

    try:
        run_background_worker(batch_size=5, sleep_between_items=(32.5, 107.5),
                              status=args.status.upper())
    except KeyboardInterrupt:
        logger.info("🛑 接收到退出信号，后台刮削引擎已安全停止。")

import logging
import sys
from pathlib import Path

# 动态引入模块
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from dal.db_manager import get_pending_actors

# 配置日志输出 (控制台 + 文件)
log_file = project_root / "data" / "logs" / "auto_actor_scraper.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_actor_worker(batch_size: int = 10, sleep_between_items: tuple = (15.0, 125.0)):
    from core.worker_client import WorkerClient
    client = WorkerClient()
    return [client.actor(name, actor_id) for actor_id, name in get_pending_actors(limit=100000)]

if __name__ == "__main__":
    # 演员刮削对封控要求相对视频稍微好一点点，但也必须保持警惕
    try:
        run_actor_worker(batch_size=10, sleep_between_items=(15.0, 125.0))
    except KeyboardInterrupt:
        logger.info("🛑 接收到退出信号，演员刮削引擎已安全停止。")

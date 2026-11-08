"""每日入库流水线守护进程。

同步扫描后提交持久化工作流，按影片刮削 → 演员刮削 → 翻译的依赖顺序推进。
外部阶段由 Consumer 异步调度，守护进程在提交后睡到下一日。

启动方式：
    nohup .venv/bin/python daily_pipeline.py > /dev/null 2>&1 &
"""

import logging
import os
import sys
import time
import traceback
from pathlib import Path

project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# 日志：data/logs/daily_pipeline.log + stdout
log_file = project_root / "data" / "logs" / "daily_pipeline.log"
log_file.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("daily_pipeline")

CYCLE_INTERVAL = 86400  # 每日一轮 (秒)


def safe_run(name, fn, *args, **kwargs):
    """跑一步，抛异常只记 log，不让整轮崩。"""
    log.info("---- [%s] 开始 ----", name)
    try:
        result = fn(*args, **kwargs)
        log.info("---- [%s] 完成 ----", name)
        return result
    except Exception:
        log.error("[%s] 异常:\n%s", name, traceback.format_exc())


def run_one_cycle():
    from core.scanner import run_scan
    from core.auto_scraper import run_background_worker
    from core.worker_client import WorkerClient

    safe_run("scanner", run_scan)
    video_tasks = safe_run("auto_scraper", run_background_worker) or []
    job_id = WorkerClient().call('daily_finalize', payload={'video_tasks': video_tasks}, priority=-1000)
    log.info("每日流水线后续阶段已入队 %s", job_id)


if __name__ == "__main__":
    log.info("============ daily_pipeline 启动 (PID %s) ============", os.getpid())
    try:
        while True:
            t0 = time.time()
            log.info(">>>>>> 开始新一轮 <<<<<<")
            run_one_cycle()
            elapsed = time.time() - t0
            remaining = max(0, CYCLE_INTERVAL - elapsed)
            log.info(">>>>>> 本轮耗时 %.0fs，距下次还有 %.0fs <<<<<<", elapsed, remaining)
            if remaining > 0:
                time.sleep(remaining)
    except KeyboardInterrupt:
        log.info("收到 SIGINT，退出。")

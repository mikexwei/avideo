#!/bin/bash
# avideo_pipeline.sh
#
# 每小时自动执行一次完整的入库流水线：
#   1. scanner        — 扫描磁盘，新文件入库为 PENDING
#   2. auto_scraper   — 刮削 PENDING 影片（最多运行 50 分钟）
#   3. auto_actor_scraper — 刮演员头像（最多运行 10 分钟）
#   4. translate_titles   — 翻译未翻译的标题（自然退出）
#
# 仅在有 PENDING 视频时才触发刮削步骤。
#
# 用法:
#   chmod +x avideo_pipeline.sh
#   ./avideo_pipeline.sh            # 前台运行（Ctrl+C 停止）
#   nohup ./avideo_pipeline.sh &    # 后台运行

set -euo pipefail

# ================= 配置区 =================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$SCRIPT_DIR/.venv/bin/python"
DB="$SCRIPT_DIR/data/avideo.db"
LOG_DIR="$SCRIPT_DIR/data/logs"
PIPELINE_LOG="$LOG_DIR/pipeline.log"
LOCK_FILE="/tmp/avideo_pipeline.lock"

SCRAPER_TIMEOUT=3000   # 50 分钟：auto_scraper 最长运行时间（秒）
ACTOR_TIMEOUT=600      # 10 分钟：auto_actor_scraper 最长运行时间（秒）
CYCLE_INTERVAL=3600    # 1 小时：每轮流水线间隔（秒）
# ==========================================

mkdir -p "$LOG_DIR"

# ---------- 工具函数 ----------

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$PIPELINE_LOG"
}

# 查询 PENDING 视频数量
pending_count() {
    sqlite3 "$DB" "SELECT COUNT(*) FROM videos WHERE scrape_status='PENDING';" 2>/dev/null || echo 0
}

# 带超时的后台运行：超时后自动杀掉子进程
# 用法: run_with_timeout <秒> <命令...>
run_with_timeout() {
    local secs=$1; shift
    "$@" >> "$PIPELINE_LOG" 2>&1 &
    local child=$!
    # 超时 watcher：在后台等待指定秒数后杀掉子进程
    ( sleep "$secs" && kill "$child" 2>/dev/null ) &
    local watcher=$!
    wait "$child" 2>/dev/null || true
    kill "$watcher" 2>/dev/null || true
    wait "$watcher" 2>/dev/null || true
}

# ---------- 锁机制：防止重叠运行 ----------

acquire_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local pid
        pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            log "WARNING: 流水线已在运行 (PID $pid)，跳过本轮。"
            exit 0
        fi
        log "发现遗留锁文件（进程 $pid 已退出），清理后继续。"
    fi
    echo $$ > "$LOCK_FILE"
}

release_lock() {
    rm -f "$LOCK_FILE"
}

trap release_lock EXIT

# ---------- 主流水线 ----------

cd "$SCRIPT_DIR"

log "================================================================"
log "avideo 流水线守护进程启动 (PID $$)"
log "Python: $PYTHON"
log "DB:     $DB"
log "================================================================"

acquire_lock

while true; do
    CYCLE_START=$(date +%s)
    log ""
    log ">>>>>> 开始新一轮流水线 <<<<<<"

    # ---- 步骤 1: 扫描 ----
    log "[1/4] 扫描本地文件..."
    if "$PYTHON" core/scanner.py >> "$PIPELINE_LOG" 2>&1; then
        log "[1/4] 扫描完成。"
    else
        log "[1/4] 扫描异常退出（退出码 $?），继续后续步骤。"
    fi

    # 检查是否有需要刮削的视频
    PENDING=$(pending_count)
    log "      当前 PENDING 视频数: $PENDING"

    if [ "$PENDING" -gt 0 ]; then
        # ---- 步骤 2: 视频刮削 ----
        log "[2/4] 运行 auto_scraper（最多 $((SCRAPER_TIMEOUT/60)) 分钟）..."
        run_with_timeout "$SCRAPER_TIMEOUT" "$PYTHON" core/auto_scraper.py
        log "[2/4] auto_scraper 结束。"

        # ---- 步骤 3: 演员刮削 ----
        log "[3/4] 运行 auto_actor_scraper（最多 $((ACTOR_TIMEOUT/60)) 分钟）..."
        run_with_timeout "$ACTOR_TIMEOUT" "$PYTHON" core/auto_actor_scraper.py
        log "[3/4] auto_actor_scraper 结束。"
    else
        log "[2/4] 无 PENDING 视频，跳过刮削步骤。"
        log "[3/4] 跳过演员刮削。"
    fi

    # ---- 步骤 4: 翻译（自然退出，无需超时）----
    log "[4/4] 运行 translate_titles..."
    if "$PYTHON" utils/translate_titles.py >> "$PIPELINE_LOG" 2>&1; then
        log "[4/4] 翻译完成。"
    else
        log "[4/4] 翻译异常退出（退出码 $?）。"
    fi

    # ---- 计算剩余休眠时间 ----
    CYCLE_END=$(date +%s)
    ELAPSED=$(( CYCLE_END - CYCLE_START ))
    REMAINING=$(( CYCLE_INTERVAL - ELAPSED ))

    log ">>>>>> 本轮流水线耗时 ${ELAPSED}s，距下次运行还有 $((REMAINING > 0 ? REMAINING : 0))s <<<<<<"

    if [ "$REMAINING" -gt 0 ]; then
        sleep "$REMAINING"
    fi
done

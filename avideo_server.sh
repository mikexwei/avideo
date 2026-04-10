#!/bin/bash

# ================= 配置区 =================
# 你可以在这里修改你的启动命令、日志文件名和 PID 文件名
START_CMD=".venv/bin/python -m flask --app web.backend.app run --host 0.0.0.0 --port 8000"
LOG_FILE="server.log"
PID_FILE="server.pid"
# ==========================================

# 启动服务
start_server() {
    # 检查是否已经存在 PID 文件
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        # 检查该 PID 的进程是否真的在运行
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "⚠️ 服务已经在运行中 (PID: $PID)，请勿重复启动！"
            return
        else
            echo "⚠️ 发现遗留的 PID 文件，但服务未运行。正在清理..."
            rm -f "$PID_FILE"
        fi
    fi

    echo "🚀 正在启动服务..."
    # 以后台静默方式运行命令，并重定向日志
    nohup $START_CMD > "$LOG_FILE" 2>&1 &
    
    # 记录最新后台进程的 PID
    echo $! > "$PID_FILE"
    echo "✅ 服务已成功启动！"
    echo "📄 日志输出至: $LOG_FILE"
    echo "🆔 进程 PID: $(cat $PID_FILE)"
}

# 停止服务
shutdown_server() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "🛑 正在停止服务 (PID: $PID)..."
            kill "$PID"
            
            # 等待进程完全退出（最多等 5 秒）
            for i in {1..5}; do
                if ! ps -p "$PID" > /dev/null 2>&1; then
                    break
                fi
                sleep 1
            done
            
            rm -f "$PID_FILE"
            echo "✅ 服务已停止。"
        else
            echo "⚠️ PID 文件存在 ($PID_FILE)，但服务并没有在运行。已清理残留文件。"
            rm -f "$PID_FILE"
        fi
    else
        echo "🤷‍♂️ 服务没有在运行 (找不到 PID 文件)。"
    fi
}

# 检查命令行参数
case "$1" in
    --start)
        start_server
        ;;
    --shutdown)
        shutdown_server
        ;;
    --restart)
        shutdown_server
        echo "⏳ 等待 2 秒以确保端口释放..."
        sleep 2
        start_server
        ;;
    *)
        # 如果输入的参数不对，提示正确用法
        echo "❌ 无效参数: $1"
        echo "💡 用法: $0 {--start | --shutdown | --restart}"
        exit 1
        ;;
esac
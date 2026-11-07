#!/bin/bash
set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT" || exit 1
PID_FILE="$ROOT/server.pid"
LOG_FILE="$ROOT/server.log"

server_pid() {
    local pid cwd command
    pid="$(lsof -nP -t -iTCP:8000 -sTCP:LISTEN 2>/dev/null | sort -u | head -n 1)"
    [ -n "$pid" ] || return 1
    cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p')"
    command="$(ps -p "$pid" -o command=)"
    if [ "$cwd" != "$ROOT" ] || [[ "$command" != *web.backend.app* ]]; then
        echo "Port 8000 is owned by another application." >&2
        return 2
    fi
    printf '%s\n' "$pid"
}

start_server() {
    local pid code
    pid="$(server_pid)"
    code=$?
    if [ "$code" -eq 0 ]; then
        printf '%s\n' "$pid" > "$PID_FILE"
        echo "Web server is already running (PID $pid)."
        return
    fi
    [ "$code" -ne 2 ] || return 1
    screen -dmS avideo-server /usr/bin/env PYTHONPATH="$ROOT" /bin/sh -c \
        'cd "$1" && exec "$2" -m flask --app web.backend.app run --host 0.0.0.0 --port 8000 >> "$3" 2>&1' \
        avideo-web "$ROOT" "$ROOT/.venv/bin/python" "$LOG_FILE"
    for _ in {1..15}; do
        if pid="$(server_pid)"; then
            printf '%s\n' "$pid" > "$PID_FILE"
            echo "Web server started (PID $pid). Worker and browser are independent."
            return
        fi
        sleep 1
    done
    echo "Web server did not start. See $LOG_FILE." >&2
    return 1
}

stop_server() {
    local pid code
    pid="$(server_pid)"
    code=$?
    [ "$code" -ne 2 ] || return 1
    if [ "$code" -ne 0 ]; then
        echo "Web server is not running."
        return
    fi
    kill "$pid"
    for _ in {1..10}; do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "Web server stopped. Worker and browser were not stopped."
            return
        fi
        sleep 1
    done
    echo "Web server is still exiting (PID $pid)." >&2
    return 1
}

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 {--start | --shutdown | --restart}" >&2
    exit 1
fi
case "$1" in
    --start) start_server ;;
    --shutdown) stop_server ;;
    --restart) stop_server && start_server ;;
    *) echo "Usage: $0 {--start | --shutdown | --restart}" >&2; exit 1 ;;
esac

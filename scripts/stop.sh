#!/bin/bash

# =========================================
#   Kafka 模拟系统 - 停止脚本
#   通过 PID 文件停止所有服务，兜底按端口清理
# =========================================

PID_DIR="/tmp/kafka-sim-pids"

echo "========================================="
echo "  Kafka 模拟系统 - 停止中..."
echo "========================================="
echo ""

# ---- 通过 PID 文件停止服务 ----
if [ -d "$PID_DIR" ]; then
    for pidfile in "$PID_DIR"/*.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            name=$(basename "$pidfile" .pid)
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null
                # 等待进程退出（最多 3 秒）
                for i in $(seq 1 6); do
                    if ! kill -0 "$pid" 2>/dev/null; then
                        break
                    fi
                    sleep 0.5
                done
                # 如果仍未退出，强制杀掉
                if kill -0 "$pid" 2>/dev/null; then
                    kill -9 "$pid" 2>/dev/null
                    echo "  [FORCE] $name (PID: $pid) 已强制停止"
                else
                    echo "  [OK]    $name (PID: $pid) 已停止"
                fi
            else
                echo "  [SKIP]  $name (PID: $pid) 已不在运行"
            fi
        fi
    done
    # 清理 PID 文件
    rm -f "$PID_DIR"/*.pid
fi

echo ""

# ---- 兜底：按端口清理残留进程 ----
echo "[CLEANUP] 检查端口残留..."
ALL_PORTS=(3000 8080 8081 8082 8083 8084 8085 8086 8087 8088 8089 8090)
for port in "${ALL_PORTS[@]}"; do
    pid=$(lsof -ti :"$port" 2>/dev/null || true)
    if [ -n "$pid" ]; then
        kill -9 "$pid" 2>/dev/null
        echo "  [FORCE] 端口 $port 上的进程 (PID: $pid) 已强制停止"
    fi
done

# ---- 清理临时文件 ----
echo ""
echo "[CLEANUP] 清理临时文件..."
rm -f /tmp/kafka-broker /tmp/kafka-orders /tmp/kafka-addresses /tmp/kafka-payments /tmp/kafka-producer
rm -rf "$PID_DIR"

echo ""
echo "========================================="
echo "  所有服务已停止"
echo "========================================="

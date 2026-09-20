#!/bin/bash

# =========================================
#   Kafka 模拟系统 - 一键启动脚本
#   启动顺序: Broker → Consumer Services → Producer/Backend → Frontend
# =========================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KAFKA_SIM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "========================================="
echo "  Kafka 模拟系统 - 启动中..."
echo "========================================="
echo ""

# ---- PID 文件目录 ----
PID_DIR="/tmp/kafka-sim-pids"
mkdir -p "$PID_DIR"

# ---- 清理端口占用 ----
# 杀掉占用指定端口的进程，确保端口可用
kill_port() {
    local port=$1
    if lsof -i :"$port" -sTCP:LISTEN > /dev/null 2>&1; then
        echo "[CLEANUP] 端口 $port 已被占用，正在停止..."
        lsof -ti :"$port" | xargs kill -9 2>/dev/null || true
        sleep 0.5
    fi
}

# 所有需要使用的端口
ALL_PORTS=(8080 8081 8082 8083 8084 8085 8086 8087 8088 8089 8090 3000)
for port in "${ALL_PORTS[@]}"; do
    kill_port "$port"
done

# ---- 构建所有 Go 服务 ----
echo "[BUILD] 编译 Go 服务..."
cd "$KAFKA_SIM_DIR"

CGO_ENABLED=0 go build -o /tmp/kafka-broker ./broker/
echo "  [OK] /tmp/kafka-broker"

CGO_ENABLED=0 go build -o /tmp/kafka-orders ./consumers/orders/
echo "  [OK] /tmp/kafka-orders"

CGO_ENABLED=0 go build -o /tmp/kafka-addresses ./consumers/addresses/
echo "  [OK] /tmp/kafka-addresses"

CGO_ENABLED=0 go build -o /tmp/kafka-payments ./consumers/payments/
echo "  [OK] /tmp/kafka-payments"

CGO_ENABLED=0 go build -o /tmp/kafka-producer ./producer/
echo "  [OK] /tmp/kafka-producer"

echo "[BUILD] 所有 Go 服务编译完成"
echo ""

# ---- 检查前端依赖 ----
echo "[BUILD] 检查前端依赖..."
cd "$KAFKA_SIM_DIR/frontend"
if [ ! -d "node_modules" ]; then
    echo "[BUILD] 安装前端依赖..."
    npm install
fi
echo "[BUILD] 前端就绪"
echo ""

# ---- 等待端口就绪的辅助函数 ----
wait_for_port() {
    local port=$1
    local name=$2
    local max_wait=10
    local waited=0
    while ! lsof -i :"$port" -sTCP:LISTEN > /dev/null 2>&1; do
        sleep 0.5
        waited=$((waited + 1))
        if [ "$waited" -ge "$max_wait" ]; then
            echo "[WARNING] $name (:${port}) 启动超时，请检查日志"
            return 1
        fi
    done
    return 0
}

# ---- 数据目录（存放消费落库的 JSON 文件） ----
DATA_DIR="$KAFKA_SIM_DIR/data"
mkdir -p "$DATA_DIR"

# ---- 1. 启动 Broker (:8081) ----
echo "[1/6] 启动 Broker (:8081)..."
/tmp/kafka-broker > "$PID_DIR/broker.log" 2>&1 &
echo $! > "$PID_DIR/broker.pid"
wait_for_port 8081 "Broker"
echo "  [OK] Broker 已启动 (PID: $(cat "$PID_DIR/broker.pid"))"

# ---- 2. 启动 Orders 消费者服务 (:8082, :8083, :8084) ----
echo "[2/6] 启动 Orders 消费者服务 (:8082, :8083, :8084)..."
/tmp/kafka-orders \
    --port-p0=8082 --port-p1=8083 --port-p2=8084 \
    --broker=http://localhost:8081 \
    --group-id=orders-group \
    --db-dir="$DATA_DIR" \
    > "$PID_DIR/orders.log" 2>&1 &
echo $! > "$PID_DIR/orders.pid"
wait_for_port 8082 "Orders-P0"
echo "  [OK] Orders 消费者服务已启动 (PID: $(cat "$PID_DIR/orders.pid"))"

# ---- 3. 启动 Addresses 消费者服务 (:8085, :8086, :8087) ----
echo "[3/6] 启动 Addresses 消费者服务 (:8085, :8086, :8087)..."
/tmp/kafka-addresses \
    --port-p0=8085 --port-p1=8086 --port-p2=8087 \
    --broker=http://localhost:8081 \
    --group-id=addresses-group \
    --db-dir="$DATA_DIR" \
    > "$PID_DIR/addresses.log" 2>&1 &
echo $! > "$PID_DIR/addresses.pid"
wait_for_port 8085 "Addresses-P0"
echo "  [OK] Addresses 消费者服务已启动 (PID: $(cat "$PID_DIR/addresses.pid"))"

# ---- 4. 启动 Payments 消费者服务 (:8088, :8089, :8090) ----
echo "[4/6] 启动 Payments 消费者服务 (:8088, :8089, :8090)..."
/tmp/kafka-payments \
    --port-p0=8088 --port-p1=8089 --port-p2=8090 \
    --broker=http://localhost:8081 \
    --group-id=payments-group \
    --db-dir="$DATA_DIR" \
    > "$PID_DIR/payments.log" 2>&1 &
echo $! > "$PID_DIR/payments.pid"
wait_for_port 8088 "Payments-P0"
echo "  [OK] Payments 消费者服务已启动 (PID: $(cat "$PID_DIR/payments.pid"))"

# ---- 5. 启动 Producer/Backend API (:8080) ----
echo "[5/6] 启动 Producer/Backend API (:8080)..."
BROKER_ADDR=http://localhost:8081 PORT=8080 \
    /tmp/kafka-producer > "$PID_DIR/producer.log" 2>&1 &
echo $! > "$PID_DIR/producer.pid"
wait_for_port 8080 "Producer/Backend"
echo "  [OK] Producer/Backend API 已启动 (PID: $(cat "$PID_DIR/producer.pid"))"

# ---- 6. 启动前端开发服务器 (:3000) ----
echo "[6/6] 启动前端开发服务器 (:3000)..."
cd "$KAFKA_SIM_DIR/frontend"
npm run dev > "$PID_DIR/frontend.log" 2>&1 &
echo $! > "$PID_DIR/frontend.pid"
wait_for_port 3000 "Frontend"
echo "  [OK] 前端开发服务器已启动 (PID: $(cat "$PID_DIR/frontend.pid"))"

# ---- 启动完成 ----
echo ""
echo "========================================="
echo "  All services are running!"
echo "========================================="
echo ""
echo "  前端 (Frontend):           http://localhost:3000"
echo "  后端 API (Producer):       http://localhost:8080"
echo "  Broker:                    http://localhost:8081"
echo "  WebSocket:                 ws://localhost:8081/ws"
echo ""
echo "  Orders 消费者服务:"
echo "    Partition 0:             http://localhost:8082"
echo "    Partition 1:             http://localhost:8083"
echo "    Partition 2:             http://localhost:8084"
echo ""
echo "  Addresses 消费者服务:"
echo "    Partition 0:             http://localhost:8085"
echo "    Partition 1:             http://localhost:8086"
echo "    Partition 2:             http://localhost:8087"
echo ""
echo "  Payments 消费者服务:"
echo "    Partition 0:             http://localhost:8088"
echo "    Partition 1:             http://localhost:8089"
echo "    Partition 2:             http://localhost:8090"
echo ""
echo "  停止所有服务:  bash $SCRIPT_DIR/stop.sh"
echo "  日志目录:      $PID_DIR/"
echo "========================================="

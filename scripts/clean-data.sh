#!/bin/bash

# =========================================
#   Kafka 模拟系统 - 清理消费数据文件
#   删除 kafka-sim/data/ 下的 JSON 文件
# =========================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/data"

echo "========================================="
echo "  清理消费数据文件..."
echo "========================================="

if [ ! -d "$DATA_DIR" ]; then
    echo "[OK] 数据目录不存在，无需清理: $DATA_DIR"
    exit 0
fi

FILES=(
    "$DATA_DIR/orders.json"
    "$DATA_DIR/addresses.json"
    "$DATA_DIR/payments.json"
)

for f in "${FILES[@]}"; do
    if [ -f "$f" ]; then
        size=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null || echo "?")
        rm -f "$f"
        echo "[CLEAN] $(basename "$f") (${size} bytes) — 已删除"
    else
        echo "[SKIP]  $(basename "$f") — 不存在"
    fi
done

echo ""
echo "========================================="
echo "  清理完成！"
echo "========================================="

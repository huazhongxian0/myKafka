package main

import (
	"fmt"
	"os"

	"kafka-sim/kafka/broker"
)

// Broker 服务入口
// 启动 Kafka Broker，监听 :8081 端口
// 提供 Produce/Fetch/Offset API 以及 WebSocket 实时推送
func main() {
	addr := os.Getenv("BROKER_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	b := broker.NewBroker()
	fmt.Println("=== Kafka Broker 启动 ===")
	fmt.Printf("监听地址: %s\n", addr)

	if err := b.StartHTTPServer(addr); err != nil {
		fmt.Fprintf(os.Stderr, "[Broker] 服务启动失败: %v\n", err)
		os.Exit(1)
	}
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-sim/database"
	"kafka-sim/kafka/consumer"
)

// 地址消费者服务入口
// 模拟 Kafka 消费者组 "addresses-group"，消费地址 topic 的 3 个 partition
// 每个 partition 对应一个独立的 HTTP 服务端口，可独立查询状态和消息
func main() {
	// 命令行参数：每个 partition 对应一个端口
	portP0 := flag.Int("port-p0", 8085, "Partition 0 的 HTTP 服务端口")
	portP1 := flag.Int("port-p1", 8086, "Partition 1 的 HTTP 服务端口")
	portP2 := flag.Int("port-p2", 8087, "Partition 2 的 HTTP 服务端口")
	broker := flag.String("broker", "http://localhost:8081", "Broker 服务地址")
	groupID := flag.String("group-id", "addresses-group", "消费者组 ID")
	dbDir := flag.String("db-dir", "./data", "数据库存储目录")
	flag.Parse()

	fmt.Println("=== 地址消费者服务启动 ===")
	fmt.Printf("Broker: %s, GroupID: %s, DB目录: %s\n", *broker, *groupID, *dbDir)

	// 创建地址消息存储（线程安全，所有 partition 共享同一个 store）
	store, err := database.NewAddressesStore(*dbDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 AddressesStore 失败: %v\n", err)
		os.Exit(1)
	}

	// 创建消费者服务，配置 topic 和轮询间隔
	cs := consumer.NewConsumerService("addresses", *groupID, "addresses", *broker, *dbDir, 2*time.Second)

	// 为 3 个 partition 分别创建监听器，共享同一个 AddressesStore
	cs.AddListener(0, store)
	cs.AddListener(1, store)
	cs.AddListener(2, store)

	// 创建可取消的 context，用于优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动所有 partition 监听器（后台 goroutine 持续拉取消息）
	cs.StartAll(ctx)

	// 为每个 partition 启动独立的 HTTP 服务器
	ports := []*int{portP0, portP1, portP2}
	partitionIDs := []int{0, 1, 2}

	for i, port := range ports {
		pid := partitionIDs[i]
		listener := cs.GetListener(pid)
		if listener == nil {
			fmt.Fprintf(os.Stderr, "找不到 partition %d 的监听器\n", pid)
			os.Exit(1)
		}

		mux := http.NewServeMux()

		// GET /status — 返回当前 partition 监听器的状态
		mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(listener.GetStatus())
		})

		// GET /metrics — 返回整个服务的聚合指标（QPS、消费总数、文件大小）
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(cs.GetStatus())
		})

		// GET /messages — 返回当前 partition 已消费的消息
		mux.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(listener.GetMessages())
		})

		addr := fmt.Sprintf(":%d", *port)
		server := &http.Server{
			Addr:    addr,
			Handler: mux,
		}

		go func(s *http.Server, p int, a string) {
			fmt.Printf("[addresses] Partition %d HTTP 服务启动: %s\n", p, a)
			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "[addresses] Partition %d HTTP 服务错误: %v\n", p, err)
			}
		}(server, pid, addr)
	}

	fmt.Println("[addresses] 所有 partition 监听器和 HTTP 服务已启动")
	fmt.Printf("[addresses] 端口映射: P0=%d, P1=%d, P2=%d\n", *portP0, *portP1, *portP2)

	// 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n[addresses] 收到退出信号，正在优雅关闭...")
	cancel()
	fmt.Println("[addresses] 地址消费者服务已停止")
}

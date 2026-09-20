package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"

	"kafka-sim/kafka/producer"
)

// 全局配置
var (
	brokerAddr string
	serverPort string
	rng        *rand.Rand
)

// BatchRequest 批量发送请求体
type BatchRequest struct {
	Count int `json:"count"`
}

// MessageDetail 单条消息的详细信息（用于返回给前端）
type MessageDetail struct {
	Iteration int    `json:"iteration"` // 第几次迭代
	RandomNum int    `json:"randomNum"` // 生成的随机数
	Topic     string `json:"topic"`     // 目标 topic
	Partition int    `json:"partition"` // 目标 partition
	Key       string `json:"key"`       // 消息 key
	Success   bool   `json:"success"`   // 是否发送成功
}

// BatchResponse 批量发送的响应结果
type BatchResponse struct {
	Total         int             `json:"total"`         // 总消息数
	OrdersCount   int             `json:"ordersCount"`   // orders topic 消息数
	AddressesCount int            `json:"addressesCount"` // addresses topic 消息数
	PaymentsCount int             `json:"paymentsCount"` // payments topic 消息数
	SuccessCount  int             `json:"successCount"`  // 成功发送数
	FailedCount   int             `json:"failedCount"`   // 发送失败数
	Details       []MessageDetail `json:"details"`       // 每条消息的详情
}

// ServiceInfo 消费者服务信息
type ServiceInfo struct {
	Name  string `json:"name"`
	Ports []int  `json:"ports"`
	Group string `json:"group"`
}

func init() {
	// 从环境变量读取配置，若未设置则使用默认值
	brokerAddr = os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "http://localhost:8081"
	}
	serverPort = os.Getenv("PORT")
	if serverPort == "" {
		serverPort = "8080"
	}
	// 使用时间戳作为随机数种子
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func main() {
	// 创建全局 Producer 实例
	p := producer.NewProducer(brokerAddr)

	mux := http.NewServeMux()

	// 注册路由
	mux.HandleFunc("/", handleHealth)
	mux.HandleFunc("/send/batch", makeBatchHandler(p))
	mux.HandleFunc("/topics", handleTopics)
	mux.HandleFunc("/services", handleServices)
	mux.HandleFunc("/consumer/metrics", handleConsumerMetrics)

	addr := ":" + serverPort
	fmt.Printf("[Producer] 启动 Producer/Backend API 服务，端口 %s\n", serverPort)
	fmt.Printf("[Producer] Broker 地址: %s\n", brokerAddr)
	fmt.Println("[Producer] API 端点:")
	fmt.Println("  GET  /                - 健康检查")
	fmt.Println("  POST /send/batch      - 批量发送消息（随机模拟）")
	fmt.Println("  GET  /topics          - 获取 topic 列表（代理到 Broker）")
	fmt.Println("  GET  /services        - 获取消费者服务信息")
	fmt.Println("  GET  /consumer/metrics - 消费者监控指标（QPS + 文件大小）")

	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "[Producer] 服务启动失败: %v\n", err)
		os.Exit(1)
	}
}

// handleHealth 健康检查
func handleHealth(w http.ResponseWriter, r *http.Request) {
	// 只处理根路径的精确匹配，避免与其他路由冲突
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"role":   "producer",
		"broker": brokerAddr,
	})
}

// makeBatchHandler 创建批量发送消息的 handler（闭包持有 Producer 引用）
func makeBatchHandler(p *producer.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 POST 方法"})
			return
		}

		// 解析请求体
		var req BatchRequest
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "读取请求体失败"})
			return
		}
		defer r.Body.Close()

		if err := json.Unmarshal(body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "JSON 解析失败"})
			return
		}

		if req.Count <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "count 必须大于 0"})
			return
		}

		// 执行批量发送
		resp := executeBatchSend(p, req.Count)
		writeJSON(w, http.StatusOK, resp)
	}
}

// executeBatchSend 核心逻辑：循环 N 次，每次生成随机数，根据随机数发送消息
func executeBatchSend(p *producer.Producer, count int) *BatchResponse {
	resp := &BatchResponse{
		Details: make([]MessageDetail, 0, count*2), // 预分配，每次迭代可能产生多条消息
	}

	// 各 topic 计数器
	ordersCount := 0
	addressesCount := 0
	paymentsCount := 0
	successCount := 0
	failedCount := 0

	for i := 0; i < count; i++ {
		// 生成 [0, 10) 的随机整数
		n := rng.Intn(10)

		// 根据随机数决定发送哪些 topic 的消息
		topics := getTopicsByRandom(n)

		for _, topic := range topics {
			// 随机选择 partition（0-2）
			partition := rng.Intn(3)
			// 生成消息 key
			key := fmt.Sprintf("%s-%d", topic[:3], i+1)
			// 生成 JSON 格式的消息体
			value := fmt.Sprintf(`{"id":%d,"topic":"%s","partition":%d,"timestamp":"%s"}`,
				i+1, topic, partition, time.Now().Format(time.RFC3339))

			// 通过 Producer 发送消息到 Broker
			err := p.SendMessage(topic, partition, key, value)
			success := err == nil

			if err != nil {
				fmt.Printf("[Producer] 发送失败: iteration=%d topic=%s partition=%d error=%v\n",
					i+1, topic, partition, err)
				failedCount++
			} else {
				successCount++
			}

			// 统计各 topic 的消息数
			switch topic {
			case "orders":
				ordersCount++
			case "addresses":
				addressesCount++
			case "payments":
				paymentsCount++
			}

			// 记录详情
			resp.Details = append(resp.Details, MessageDetail{
				Iteration: i + 1,
				RandomNum: n,
				Topic:     topic,
				Partition: partition,
				Key:       key,
				Success:   success,
			})
		}
	}

	resp.Total = ordersCount + addressesCount + paymentsCount
	resp.OrdersCount = ordersCount
	resp.AddressesCount = addressesCount
	resp.PaymentsCount = paymentsCount
	resp.SuccessCount = successCount
	resp.FailedCount = failedCount

	fmt.Printf("[Producer] 批量发送完成: count=%d total=%d orders=%d addresses=%d payments=%d success=%d failed=%d\n",
		count, resp.Total, ordersCount, addressesCount, paymentsCount, successCount, failedCount)

	return resp
}

// getTopicsByRandom 根据随机数决定要发送的 topic 列表
//
//	n=0,1 → orders
//	n=2,3 → addresses
//	n=4,5 → payments
//	n=6   → orders + addresses
//	n=7   → orders + payments
//	n=8   → addresses + payments
//	n=9   → orders + addresses + payments
func getTopicsByRandom(n int) []string {
	switch n {
	case 0, 1:
		return []string{"orders"}
	case 2, 3:
		return []string{"addresses"}
	case 4, 5:
		return []string{"payments"}
	case 6:
		return []string{"orders", "addresses"}
	case 7:
		return []string{"orders", "payments"}
	case 8:
		return []string{"addresses", "payments"}
	case 9:
		return []string{"orders", "addresses", "payments"}
	default:
		// 兜底，不应发生
		return []string{"orders"}
	}
}

// handleTopics 代理到 Broker 的 /topics 端点
func handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET 方法"})
		return
	}

	// 向 Broker 发起请求获取 topic 列表
	resp, err := http.Get(brokerAddr + "/topics")
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("无法连接 Broker: %v", err)})
		return
	}
	defer resp.Body.Close()

	// 将 Broker 的响应原样转发
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "读取 Broker 响应失败"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// handleServices 返回所有消费者服务的信息
func handleServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET 方法"})
		return
	}

	services := map[string]ServiceInfo{
		"orders": {
			Name:  "orders",
			Ports: []int{8082, 8083, 8084},
			Group: "orders-group",
		},
		"addresses": {
			Name:  "addresses",
			Ports: []int{8085, 8086, 8087},
			Group: "addresses-group",
		},
		"payments": {
			Name:  "payments",
			Ports: []int{8088, 8089, 8090},
			Group: "payments-group",
		},
	}

	writeJSON(w, http.StatusOK, services)
}

// writeJSON 通用的 JSON 响应写入函数
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ServiceMetrics 单个消费者服务的监控指标
type ServiceMetrics struct {
	QPS           float64 `json:"qps"`
	ConsumedCount int64   `json:"consumedCount"`
	FileSize      int64   `json:"fileSize"`
	FileSizeHuman string  `json:"fileSizeHuman"`
}

// handleConsumerMetrics 聚合所有消费者服务的 QPS、消费数和文件大小
func handleConsumerMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET 方法"})
		return
	}

	// 每个服务的端口映射
	servicePorts := map[string][]int{
		"orders":    {8082, 8083, 8084},
		"addresses": {8085, 8086, 8087},
		"payments":  {8088, 8089, 8090},
	}

	metrics := make(map[string]ServiceMetrics)

	for svcName, ports := range servicePorts {
		var totalQPS float64
		var totalConsumed int64
		var totalFileSize int64

		// 遍历每个 partition 端口，汇总数据（取第一个端口的 /metrics 即可，聚合数据各端口一致）
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", ports[0]))
		if err == nil {
			body, err2 := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err2 == nil {
				var status map[string]interface{}
				if err3 := json.Unmarshal(body, &status); err3 == nil {
					if qps, ok := status["totalQPS"].(float64); ok {
						totalQPS = qps
					}
					if consumed, ok := status["totalConsumed"].(float64); ok {
						totalConsumed = int64(consumed)
					}
					if fs, ok := status["fileSize"].(float64); ok {
						totalFileSize = int64(fs)
					}
				}
			}
		}

		metrics[svcName] = ServiceMetrics{
			QPS:           totalQPS,
			ConsumedCount: totalConsumed,
			FileSize:      totalFileSize,
			FileSizeHuman: fmtHumanSize(totalFileSize),
		}
	}

	writeJSON(w, http.StatusOK, metrics)
}

// fmtHumanSize 将字节数转换为人类可读的大小字符串
func fmtHumanSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"kafka-sim/common"
)

// Broker 模拟 Kafka Broker，管理所有 topic、consumer offset 和 WebSocket 广播
type Broker struct {
	mu               sync.RWMutex
	topics           map[string]*Topic
	committedOffsets map[string]map[string]int64 // groupID -> "topic-partition" -> offset
	wsHub            *WSHub
}

// NewBroker 创建一个新的 Broker 实例，并自动创建默认的 topic
// 默认 topic: orders, addresses, payments，各 3 个 partition
func NewBroker() *Broker {
	b := &Broker{
		topics:           make(map[string]*Topic),
		committedOffsets: make(map[string]map[string]int64),
		wsHub:            NewWSHub(),
	}

	// 自动创建默认 topic
	defaultTopics := []string{"orders", "addresses", "payments"}
	for _, name := range defaultTopics {
		b.topics[name] = NewTopic(name, 3)
		fmt.Printf("[Broker] Topic auto-created: %s (partitions=3)\n", name)
	}

	return b
}

// GetOrCreateTopic 获取已有 topic 或创建新的（3 个 partition）
func (b *Broker) GetOrCreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		return t
	}
	t := NewTopic(name, 3)
	b.topics[name] = t
	fmt.Printf("[Broker] Topic created: %s (partitions=3)\n", name)
	return t
}

// Produce 向指定 topic 的指定 partition 写入消息
func (b *Broker) Produce(req *common.ProduceRequest) (*common.ProduceResponse, error) {
	topic := b.GetOrCreateTopic(req.Topic)
	partition, err := topic.GetPartition(req.Partition)
	if err != nil {
		return nil, err
	}

	msg := &common.Message{
		Key:       req.Key,
		Value:     req.Value,
		Topic:     req.Topic,
		Partition: req.Partition,
	}

	offset := partition.Append(msg)
	fmt.Printf("[Broker] Message produced: topic=%s partition=%d offset=%d key=%s\n",
		req.Topic, req.Partition, offset, req.Key)

	// 通过 WebSocket 广播新消息事件
	b.wsHub.BroadcastJSON(map[string]interface{}{
		"type":      "message",
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       msg.Key,
		"value":     msg.Value,
		"timestamp": msg.Timestamp,
	})

	// 广播队列状态变更
	b.wsHub.BroadcastJSON(b.GetQueueStatus())

	return &common.ProduceResponse{
		Offset:    offset,
		Partition: req.Partition,
		Success:   true,
	}, nil
}

// Fetch 从指定 topic 的指定 partition 拉取消息
func (b *Broker) Fetch(req *common.FetchRequest) (*common.FetchResponse, error) {
	b.mu.RLock()
	topic, ok := b.topics[req.Topic]
	b.mu.RUnlock()

	if !ok {
		return &common.FetchResponse{Messages: nil, HighWatermark: 0}, nil
	}

	partition, err := topic.GetPartition(req.Partition)
	if err != nil {
		return nil, err
	}

	messages, highWatermark := partition.Fetch(req.Offset, req.MaxBytes)
	return &common.FetchResponse{
		Messages:      messages,
		HighWatermark: highWatermark,
	}, nil
}

// CommitOffset 提交 consumer group 的消费进度
func (b *Broker) CommitOffset(req *common.OffsetCommitRequest) (*common.OffsetCommitResponse, error) {
	b.mu.Lock()

	key := fmt.Sprintf("%s-%d", req.Topic, req.Partition)
	if _, ok := b.committedOffsets[req.GroupID]; !ok {
		b.committedOffsets[req.GroupID] = make(map[string]int64)
	}
	b.committedOffsets[req.GroupID][key] = req.Offset

	fmt.Printf("[Broker] Offset committed: group=%s topic=%s partition=%d offset=%d\n",
		req.GroupID, req.Topic, req.Partition, req.Offset)

	// 广播 offset 变更事件
	b.wsHub.BroadcastJSON(map[string]interface{}{
		"type":      "offset",
		"groupId":   req.GroupID,
		"topic":     req.Topic,
		"partition": req.Partition,
		"offset":    req.Offset,
	})

	b.mu.Unlock()

	// 广播队列状态变更
	b.wsHub.BroadcastJSON(b.GetQueueStatus())

	return &common.OffsetCommitResponse{Success: true}, nil
}

// FetchOffset 查询 consumer group 已提交的 offset
func (b *Broker) FetchOffset(req *common.OffsetFetchRequest) (*common.OffsetFetchResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", req.Topic, req.Partition)
	var offset int64
	if groupOffsets, ok := b.committedOffsets[req.GroupID]; ok {
		if off, ok := groupOffsets[key]; ok {
			offset = off
		}
	}
	return &common.OffsetFetchResponse{Offset: offset}, nil
}

// GetTopics 返回所有 topic 的详情列表
func (b *Broker) GetTopics() []common.TopicDetails {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []common.TopicDetails
	for _, topic := range b.topics {
		info := common.TopicDetails{
			Name:       topic.Name,
			Partitions: make([]common.PartitionInfo, len(topic.Partitions)),
		}
		for i, p := range topic.Partitions {
			info.Partitions[i] = common.PartitionInfo{
				Partition:     i,
				MessageCount:  p.Size(),
				HighWatermark: p.Size(),
			}
		}
		result = append(result, info)
	}
	return result
}

// GetOffsets 获取所有 consumer group 的 offset（深拷贝）
func (b *Broker) GetOffsets() map[string]map[string]int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make(map[string]map[string]int64)
	for k, v := range b.committedOffsets {
		inner := make(map[string]int64)
		for kk, vv := range v {
			inner[kk] = vv
		}
		result[k] = inner
	}
	return result
}

// GetQueueStatus 返回每个 partition 的队列状态
// 包含 high watermark、每个 consumer group 的 committed offset 和 pending 数
func (b *Broker) GetQueueStatus() map[string]interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	type PartitionQueue struct {
		HighWatermark    int64            `json:"highWatermark"`
		CommittedByGroup map[string]int64 `json:"committedByGroup"`
		Pending          int64            `json:"pending"`
	}

	topics := make(map[string][]PartitionQueue)
	for name, topic := range b.topics {
		var partitions []PartitionQueue
		for i, p := range topic.Partitions {
			hw := p.Size()
			committed := make(map[string]int64)
			for gid, offsets := range b.committedOffsets {
				key := fmt.Sprintf("%s-%d", name, i)
				if off, ok := offsets[key]; ok {
					committed[gid] = off
				}
			}
			// pending = highWatermark - 最大 committed offset
			maxCommitted := int64(0)
			for _, off := range committed {
				if off > maxCommitted {
					maxCommitted = off
				}
			}
			pending := hw - maxCommitted
			if pending < 0 {
				pending = 0
			}
			partitions = append(partitions, PartitionQueue{
				HighWatermark:    hw,
				CommittedByGroup: committed,
				Pending:          pending,
			})
		}
		topics[name] = partitions
	}

	return map[string]interface{}{
		"type":   "queue",
		"topics": topics,
	}
}

// GetWSHub 返回 WebSocket Hub，供外部使用
func (b *Broker) GetWSHub() *WSHub {
	return b.wsHub
}

// getSnapshotJSON 生成当前状态快照的 JSON 字节
func (b *Broker) getSnapshotJSON() []byte {
	topics := b.GetTopics()
	offsets := b.GetOffsets()
	data, _ := json.Marshal(map[string]interface{}{
		"type":    "snapshot",
		"topics":  topics,
		"offsets": offsets,
	})
	return data
}

// --- HTTP 辅助函数 ---

func readJSON(r *http.Request, v interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	return json.Unmarshal(body, v)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// --- HTTP Handler 方法 ---

// HandleProduce 处理 POST /produce 请求
func (b *Broker) HandleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req common.ProduceRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	resp, err := b.Produce(&req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HandleFetch 处理 POST /fetch 请求
func (b *Broker) HandleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req common.FetchRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	resp, err := b.Fetch(&req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HandleOffsetCommit 处理 POST /offset/commit 请求
func (b *Broker) HandleOffsetCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req common.OffsetCommitRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	resp, err := b.CommitOffset(&req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HandleOffsetFetch 处理 POST /offset/fetch 请求
func (b *Broker) HandleOffsetFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req common.OffsetFetchRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	resp, err := b.FetchOffset(&req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HandleTopics 处理 GET /topics 请求
func (b *Broker) HandleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topics := b.GetTopics()
	writeJSON(w, http.StatusOK, topics)
}

// HandleWebSocket 处理 GET /ws WebSocket 连接请求
func (b *Broker) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	b.wsHub.HandleWebSocket(w, r, b.getSnapshotJSON)
}

// RegisterHTTPHandlers 将所有 Broker 的 HTTP handler 注册到指定的 ServeMux
func (b *Broker) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/produce", b.HandleProduce)
	mux.HandleFunc("/fetch", b.HandleFetch)
	mux.HandleFunc("/offset/commit", b.HandleOffsetCommit)
	mux.HandleFunc("/offset/fetch", b.HandleOffsetFetch)
	mux.HandleFunc("/topics", b.HandleTopics)
	mux.HandleFunc("/ws", b.HandleWebSocket)
}

// StartHTTPServer 启动 Broker 的 HTTP 服务器
// 自动启动 WebSocket Hub 并注册所有 endpoint
func (b *Broker) StartHTTPServer(addr string) error {
	// 启动 WebSocket Hub
	go b.wsHub.Run()

	mux := http.NewServeMux()
	b.RegisterHTTPHandlers(mux)

	fmt.Printf("[Broker] Starting Kafka Broker on %s\n", addr)
	fmt.Println("[Broker] APIs:")
	fmt.Println("  POST /produce        - produce message")
	fmt.Println("  POST /fetch          - fetch messages")
	fmt.Println("  POST /offset/commit  - commit consumer offset")
	fmt.Println("  POST /offset/fetch   - fetch committed offset")
	fmt.Println("  GET  /topics         - list topics")
	fmt.Println("  WS   /ws             - websocket for real-time events")

	return http.ListenAndServe(addr, mux)
}

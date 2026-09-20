package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"kafka-sim/common"
)

// ConsumerService 管理一个消费者服务的所有 partition 监听器
// 一个 ConsumerService 对应一个 consumer group，消费一个 topic 的所有（或部分）partition
type ConsumerService struct {
	serviceName  string
	groupID      string
	topic        string
	brokerAddr   string
	pollInterval time.Duration
	dbDir        string

	mu        sync.RWMutex
	listeners []*PartitionListener
}

// NewConsumerService 创建一个新的消费者服务
func NewConsumerService(serviceName, groupID, topic, brokerAddr, dbDir string, pollInterval time.Duration) *ConsumerService {
	return &ConsumerService{
		serviceName:  serviceName,
		groupID:      groupID,
		topic:        topic,
		brokerAddr:   brokerAddr,
		dbDir:        dbDir,
		pollInterval: pollInterval,
		listeners:    make([]*PartitionListener, 0),
	}
}

// AddListener 为指定的 partition 创建并注册一个监听器
func (cs *ConsumerService) AddListener(partitionID int, db DBStore) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	consumerID := fmt.Sprintf("%s-p%d", cs.serviceName, partitionID)
	listener := NewPartitionListener(consumerID, cs.groupID, cs.topic, partitionID, cs.brokerAddr, db)
	cs.listeners = append(cs.listeners, listener)
}

// StartAll 启动所有已注册的监听器
func (cs *ConsumerService) StartAll(ctx context.Context) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for _, listener := range cs.listeners {
		go listener.Start(ctx, cs.pollInterval)
	}
	fmt.Printf("[%s] All %d partition listeners started\n", cs.serviceName, len(cs.listeners))
}

// RegisterHTTPHandlers 将消费者服务的 HTTP handler 注册到指定的 ServeMux
// 注册的路径: /status, /messages, /offsets
func (cs *ConsumerService) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/status", cs.handleStatus)
	mux.HandleFunc("/messages", cs.handleMessages)
	mux.HandleFunc("/offsets", cs.handleOffsets)
}

// handleStatus 处理 GET /status 请求
func (cs *ConsumerService) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := cs.GetStatus()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleMessages 处理 GET /messages 请求
func (cs *ConsumerService) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	messages := cs.GetAllMessages()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}

// handleOffsets 处理 GET /offsets 请求
func (cs *ConsumerService) handleOffsets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	offsets := make(map[string]interface{})
	for _, listener := range cs.listeners {
		status := listener.GetStatus()
		key := fmt.Sprintf("%s-%d", cs.topic, listener.partitionID)
		offsets[key] = status["currentOffset"]
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(offsets)
}

// GetStatus 获取消费者服务的整体状态
func (cs *ConsumerService) GetStatus() map[string]interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	totalMessages := 0
	totalConsumed := int64(0)
	totalQPS := 0.0
	listenerStatuses := make([]map[string]interface{}, 0, len(cs.listeners))

	for _, listener := range cs.listeners {
		status := listener.GetStatus()
		listenerStatuses = append(listenerStatuses, status)
		if count, ok := status["messageCount"].(int); ok {
			totalMessages += count
		}
		if count, ok := status["consumedCount"].(int64); ok {
			totalConsumed += count
		}
		if qps, ok := status["qps"].(float64); ok {
			totalQPS += qps
		}
	}

	return map[string]interface{}{
		"serviceName":   cs.serviceName,
		"groupId":       cs.groupID,
		"topic":         cs.topic,
		"totalMessages": totalMessages,
		"totalConsumed": totalConsumed,
		"totalQPS":      totalQPS,
		"fileSize":      cs.getFileSize(),
		"listeners":     listenerStatuses,
	}
}

// GetListener 获取指定 partition 的监听器
func (cs *ConsumerService) GetListener(partitionID int) *PartitionListener {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for _, listener := range cs.listeners {
		if listener.partitionID == partitionID {
			return listener
		}
	}
	return nil
}

// GetListeners 获取所有监听器
func (cs *ConsumerService) GetListeners() []*PartitionListener {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make([]*PartitionListener, len(cs.listeners))
	copy(result, cs.listeners)
	return result
}

// GetAllMessages 获取所有监听器已消费的消息
func (cs *ConsumerService) GetAllMessages() []common.Message {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var allMessages []common.Message
	for _, listener := range cs.listeners {
		allMessages = append(allMessages, listener.GetMessages()...)
	}
	return allMessages
}

// getFileSize 获取该服务的数据库文件大小
func (cs *ConsumerService) getFileSize() int64 {
	filePath := filepath.Join(cs.dbDir, cs.serviceName+".json")
	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}
	return info.Size()
}

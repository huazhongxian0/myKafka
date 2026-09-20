package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"kafka-sim/common"
)

// DBStore 定义消息持久化存储接口
// 由具体的 store 实现（orders_store, addresses_store, payments_store）
type DBStore interface {
	StoreMessages(msgs []common.Message) error
	GetMessages() ([]common.Message, error)
}

// fetchRecord 记录一次 fetch 的时间和消费条数
type fetchRecord struct {
	ts    time.Time
	count int
}

// PartitionListener 单个 partition 的消费监听器
// 类似 Kafka 的 Fetcher，负责从一个指定的 partition 持续拉取消息
type PartitionListener struct {
	consumerID  string
	groupID     string
	topic       string
	partitionID int
	brokerAddr  string

	mu            sync.RWMutex
	currentOffset int64
	messages      []common.Message

	dbStore       DBStore
	httpClient    *http.Client

	// QPS 统计：滑动窗口（最近 5 秒的消费消息数）
	fetchRecords  []fetchRecord // 最近 fetch 的时间和条数
	consumedCount int64         // 累计消费总数
}

// NewPartitionListener 创建一个新的 partition 监听器
func NewPartitionListener(consumerID string, groupID string, topic string, partitionID int, brokerAddr string, db DBStore) *PartitionListener {
	return &PartitionListener{
		consumerID:  consumerID,
		groupID:     groupID,
		topic:       topic,
		partitionID: partitionID,
		brokerAddr:  brokerAddr,
		messages:    make([]common.Message, 0),
		dbStore:     db,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Start 启动监听器的主循环，定期从 broker 拉取消息
func (pl *PartitionListener) Start(ctx context.Context, pollInterval time.Duration) {
	// 初始化时从 broker 获取已提交的 offset
	offset, err := pl.fetchCommittedOffset()
	if err != nil {
		fmt.Printf("[%s] Warning: failed to fetch committed offset for %s-%d: %v, starting from 0\n",
			pl.consumerID, pl.topic, pl.partitionID, err)
		offset = 0
	}
	pl.mu.Lock()
	pl.currentOffset = offset
	pl.mu.Unlock()

	fmt.Printf("[%s] PartitionListener started: %s partition=%d, starting offset=%d\n",
		pl.consumerID, pl.topic, pl.partitionID, offset)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] PartitionListener stopping: %s partition=%d\n",
				pl.consumerID, pl.topic, pl.partitionID)
			return
		case <-ticker.C:
			pl.fetchMessages()
		}
	}
}

// fetchMessages 从 broker 拉取新消息并处理
func (pl *PartitionListener) fetchMessages() {
	pl.mu.RLock()
	offset := pl.currentOffset
	pl.mu.RUnlock()

	req := common.FetchRequest{
		Topic:     pl.topic,
		Partition: pl.partitionID,
		Offset:    offset,
		MaxBytes:  100,
	}

	body, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("[%s] Failed to marshal fetch request: %v\n", pl.consumerID, err)
		return
	}

	resp, err := pl.httpClient.Post(pl.brokerAddr+"/fetch", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Printf("[%s] Fetch error for %s-%d: %v\n", pl.consumerID, pl.topic, pl.partitionID, err)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("[%s] Failed to read fetch response: %v\n", pl.consumerID, err)
		return
	}

	var fetchResp common.FetchResponse
	if err := json.Unmarshal(respBody, &fetchResp); err != nil {
		fmt.Printf("[%s] Failed to parse fetch response: %v\n", pl.consumerID, err)
		return
	}

	if len(fetchResp.Messages) == 0 {
		return
	}

	fmt.Printf("[%s] Fetched %d messages from %s partition %d (offset %d -> %d)\n",
		pl.consumerID, len(fetchResp.Messages), pl.topic, pl.partitionID,
		offset, fetchResp.Messages[len(fetchResp.Messages)-1].Offset)

	// 将消息存入本地数据库
	msgs := make([]common.Message, len(fetchResp.Messages))
	for i, m := range fetchResp.Messages {
		msgs[i] = *m
	}

	if err := pl.dbStore.StoreMessages(msgs); err != nil {
		fmt.Printf("[%s] Failed to store messages: %v\n", pl.consumerID, err)
	}

	// 更新本地消息列表
	pl.mu.Lock()
	pl.messages = append(pl.messages, msgs...)
	// QPS 统计：记录本次 fetch 的时间和条数（滑动窗口）
	pl.fetchRecords = append(pl.fetchRecords, fetchRecord{ts: time.Now(), count: len(msgs)})
	pl.consumedCount += int64(len(msgs))
	// 清理超过 5 秒的旧记录
	cutoff := time.Now().Add(-5 * time.Second)
	for len(pl.fetchRecords) > 0 && pl.fetchRecords[0].ts.Before(cutoff) {
		pl.fetchRecords = pl.fetchRecords[1:]
	}
	pl.mu.Unlock()

	// 提交新的 offset 到 broker
	newOffset := fetchResp.Messages[len(fetchResp.Messages)-1].Offset + 1
	if err := pl.commitOffset(newOffset); err != nil {
		fmt.Printf("[%s] Failed to commit offset: %v\n", pl.consumerID, err)
		return
	}

	// 更新当前 offset
	pl.mu.Lock()
	pl.currentOffset = newOffset
	pl.mu.Unlock()
}

// commitOffset 向 broker 提交消费进度
func (pl *PartitionListener) commitOffset(offset int64) error {
	req := common.OffsetCommitRequest{
		GroupID:   pl.groupID,
		Topic:     pl.topic,
		Partition: pl.partitionID,
		Offset:    offset,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	resp, err := pl.httpClient.Post(pl.brokerAddr+"/offset/commit", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	var commitResp common.OffsetCommitResponse
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response error: %w", err)
	}

	if err := json.Unmarshal(respBody, &commitResp); err != nil {
		return fmt.Errorf("unmarshal error: %w", err)
	}

	if !commitResp.Success {
		return fmt.Errorf("commit offset failed")
	}

	return nil
}

// fetchCommittedOffset 从 broker 获取已提交的 offset
func (pl *PartitionListener) fetchCommittedOffset() (int64, error) {
	req := common.OffsetFetchRequest{
		GroupID:   pl.groupID,
		Topic:     pl.topic,
		Partition: pl.partitionID,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal error: %w", err)
	}

	resp, err := pl.httpClient.Post(pl.brokerAddr+"/offset/fetch", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response error: %w", err)
	}

	var offsetResp common.OffsetFetchResponse
	if err := json.Unmarshal(respBody, &offsetResp); err != nil {
		return 0, fmt.Errorf("unmarshal error: %w", err)
	}

	return offsetResp.Offset, nil
}

// GetStatus 获取当前监听器的状态信息
func (pl *PartitionListener) GetStatus() map[string]interface{} {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	qps := pl.calcQPS()
	return map[string]interface{}{
		"consumerId":    pl.consumerID,
		"groupId":       pl.groupID,
		"topic":         pl.topic,
		"partition":     pl.partitionID,
		"currentOffset": pl.currentOffset,
		"messageCount":  len(pl.messages),
		"qps":           qps,
		"consumedCount": pl.consumedCount,
	}
}

// GetMessages 获取当前监听器已消费的所有消息
func (pl *PartitionListener) GetMessages() []common.Message {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]common.Message, len(pl.messages))
	copy(result, pl.messages)
	return result
}

// calcQPS 计算 QPS（基于最近 5 秒滑动窗口内的消费消息数）
// 调用者必须持有读锁
func (pl *PartitionListener) calcQPS() float64 {
	if len(pl.fetchRecords) == 0 {
		return 0
	}
	cutoff := time.Now().Add(-5 * time.Second)
	totalMsgs := 0
	for _, r := range pl.fetchRecords {
		if r.ts.After(cutoff) {
			totalMsgs += r.count
		}
	}
	if totalMsgs == 0 {
		return 0
	}
	// QPS = 最近 5 秒消费的消息数 / 5
	return float64(totalMsgs) / 5.0
}

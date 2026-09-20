package common

import (
	"fmt"
	"strconv"
)

// Message 表示 Kafka 中的一条消息记录
type Message struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
}

// TopicPartition 唯一标识一个 topic 的某个 partition
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

func (tp TopicPartition) String() string {
	return tp.Topic + "-" + strconv.Itoa(tp.Partition)
}

// Key 用于 map key
func (tp TopicPartition) Key() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

// --- Broker API 请求/响应 ---

// ProduceRequest 写入消息请求
type ProduceRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// ProduceResponse 写入消息响应
type ProduceResponse struct {
	Offset    int64  `json:"offset"`
	Partition int    `json:"partition"`
	Success   bool   `json:"success"`
}

// FetchRequest 拉取消息请求
type FetchRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxBytes  int    `json:"maxBytes"`
}

// FetchResponse 拉取消息响应
type FetchResponse struct {
	Messages      []*Message `json:"messages"`
	HighWatermark int64      `json:"highWatermark"`
}

// OffsetCommitRequest 提交消费进度请求
type OffsetCommitRequest struct {
	GroupID   string `json:"groupId"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// OffsetCommitResponse 提交消费进度响应
type OffsetCommitResponse struct {
	Success bool `json:"success"`
}

// OffsetFetchRequest 查询已提交的 offset 请求
type OffsetFetchRequest struct {
	GroupID   string `json:"groupId"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// OffsetFetchResponse 查询已提交的 offset 响应
type OffsetFetchResponse struct {
	Offset int64 `json:"offset"`
}

// TopicDetails topic 元信息（含 partition 详情）
type TopicDetails struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

// PartitionInfo partition 元信息
type PartitionInfo struct {
	Partition     int   `json:"partition"`
	MessageCount  int64 `json:"messageCount"`
	HighWatermark int64 `json:"highWatermark"`
}

// TopicInfo 向后兼容的 topic 信息别名
type TopicInfo = TopicDetails

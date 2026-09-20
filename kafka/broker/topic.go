package broker

import (
	"fmt"
	"sync"
)

// Topic 表示一个 topic，包含多个 partition
// 每个 topic 有自己的名称和固定数量的 partition
type Topic struct {
	Name       string
	Partitions []*Partition
	mu         sync.RWMutex
}

// NewTopic 创建一个新的 topic，指定 partition 数量
func NewTopic(name string, numPartitions int) *Topic {
	t := &Topic{
		Name:       name,
		Partitions: make([]*Partition, numPartitions),
	}
	for i := 0; i < numPartitions; i++ {
		t.Partitions[i] = NewPartition()
	}
	return t
}

// GetPartition 根据 id 获取指定的 partition
// 如果 id 超出范围，返回错误
func (t *Topic) GetPartition(id int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if id < 0 || id >= len(t.Partitions) {
		return nil, fmt.Errorf("partition %d out of range [0, %d)", id, len(t.Partitions))
	}
	return t.Partitions[id], nil
}

package broker

import (
	"sync"
	"time"

	"kafka-sim/common"
)

// Partition 表示一个 topic partition，内部是 append-only log
// 模拟 Kafka partition 的核心设计：消息只能追加，不能修改或删除
type Partition struct {
	mu       sync.RWMutex
	messages []*common.Message
}

// NewPartition 创建一个新的空 partition
func NewPartition() *Partition {
	return &Partition{
		messages: make([]*common.Message, 0),
	}
}

// Append 向 partition 追加一条消息，自动分配 offset 和时间戳
// 返回分配的 offset 值
func (p *Partition) Append(msg *common.Message) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	msg.Offset = int64(len(p.messages))
	msg.Timestamp = time.Now().UnixMilli()
	p.messages = append(p.messages, msg)
	return msg.Offset
}

// Fetch 从指定 offset 开始拉取消息，最多拉取 maxBytes 条
// 返回消息列表和当前 high watermark（下一个可用的 offset）
func (p *Partition) Fetch(fromOffset int64, maxBytes int) ([]*common.Message, int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	highWatermark := int64(len(p.messages))
	if fromOffset >= highWatermark {
		return nil, highWatermark
	}

	var result []*common.Message
	count := 0
	for i := fromOffset; i < highWatermark; i++ {
		result = append(result, p.messages[i])
		count++
		if maxBytes > 0 && count >= maxBytes {
			break
		}
	}
	return result, highWatermark
}

// Size 返回 partition 中当前消息总数（即 high watermark）
func (p *Partition) Size() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(len(p.messages))
}

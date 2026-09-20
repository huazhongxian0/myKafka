package database

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"kafka-sim/common"
)

// PaymentsStore 支付服务的消息存储
// 将消费到的支付消息持久化到本地 JSON 文件
type PaymentsStore struct {
	mu       sync.Mutex
	filePath string
	messages []common.Message
}

// NewPaymentsStore 创建一个新的支付消息存储实例
// 自动从文件加载已有数据
func NewPaymentsStore(dbDir string) (*PaymentsStore, error) {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据库目录失败: %w", err)
	}

	filePath := filepath.Join(dbDir, "payments.json")
	store := &PaymentsStore{
		filePath: filePath,
		messages: make([]common.Message, 0),
	}

	if err := store.load(); err != nil {
		fmt.Printf("[PaymentsStore] 加载已有数据: %v\n", err)
	}

	return store, nil
}

// load 从文件加载消息
func (s *PaymentsStore) load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	if err := json.Unmarshal(data, &s.messages); err != nil {
		return fmt.Errorf("解析 payments.json 失败: %w", err)
	}

	fmt.Printf("[PaymentsStore] 加载了 %d 条消息 from %s\n", len(s.messages), s.filePath)
	return nil
}

// save 保存消息到文件
func (s *PaymentsStore) save() error {
	data, err := json.MarshalIndent(s.messages, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	if err := os.WriteFile(s.filePath, data, 0644); err != nil {
		return fmt.Errorf("写入 payments.json 失败: %w", err)
	}

	return nil
}

// StoreMessages 存储消息到本地文件
func (s *PaymentsStore) StoreMessages(msgs []common.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages = append(s.messages, msgs...)

	if err := s.save(); err != nil {
		return fmt.Errorf("保存消息失败: %w", err)
	}

	fmt.Printf("[PaymentsStore] 存储了 %d 条消息 (总计: %d)\n", len(msgs), len(s.messages))
	return nil
}

// GetMessages 获取所有已存储的消息
func (s *PaymentsStore) GetMessages() ([]common.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]common.Message, len(s.messages))
	copy(result, s.messages)
	return result, nil
}

// Count 返回已存储的消息数量
func (s *PaymentsStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

package producer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"kafka-sim/common"
)

// BatchItem 批量发送时的单条消息项
type BatchItem struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// BatchResult 批量发送的结果统计
type BatchResult struct {
	Total   int `json:"total"`
	Success int `json:"success"`
	Failed  int `json:"failed"`
}

// Producer Kafka 消息生产者
// 通过 HTTP 与 Broker 通信，发送消息到指定的 topic 和 partition
type Producer struct {
	brokerAddr string
	httpClient *http.Client
}

// NewProducer 创建一个新的 Producer 实例
func NewProducer(brokerAddr string) *Producer {
	return &Producer{
		brokerAddr: brokerAddr,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// SendMessage 发送单条消息到指定的 topic 和 partition
func (p *Producer) SendMessage(topic string, partition int, key, value string) error {
	req := common.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}

	resp, err := p.httpClient.Post(p.brokerAddr+"/produce", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %w", err)
	}

	var produceResp common.ProduceResponse
	if err := json.Unmarshal(respBody, &produceResp); err != nil {
		return fmt.Errorf("解析响应失败: %w", err)
	}

	if !produceResp.Success {
		return fmt.Errorf("broker 返回发送失败")
	}

	fmt.Printf("[Producer] Message sent: topic=%s partition=%d offset=%d key=%s\n",
		topic, partition, produceResp.Offset, key)

	return nil
}

// SendBatch 批量发送消息
// 返回发送结果统计（总数、成功数、失败数）
func (p *Producer) SendBatch(messages []BatchItem) (*BatchResult, error) {
	result := &BatchResult{
		Total: len(messages),
	}

	for _, item := range messages {
		err := p.SendMessage(item.Topic, item.Partition, item.Key, item.Value)
		if err != nil {
			fmt.Printf("[Producer] Failed to send message: topic=%s partition=%d key=%s error=%v\n",
				item.Topic, item.Partition, item.Key, err)
			result.Failed++
		} else {
			result.Success++
		}
	}

	fmt.Printf("[Producer] Batch send completed: total=%d success=%d failed=%d\n",
		result.Total, result.Success, result.Failed)

	return result, nil
}

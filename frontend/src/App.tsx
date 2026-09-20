import { useState, useCallback, useRef, useEffect } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { Message, QueueStatus, ServiceName, ServiceMetricsData } from './types'
import { SERVICES, SERVICE_COLORS, SERVICE_LABELS, TOPICS, PARTITIONS_PER_TOPIC } from './types'
import { useWebSocket, sendBatch, fetchConsumerStatus, fetchConsumerMetrics } from './api'
import { ArchitectureDiagram } from './ArchitectureDiagram'
import './App.css'

interface LogEntry {
  id: number
  time: string
  service: string
  color: string
  text: string
}

interface PartitionQueue {
  highWatermark: number
  committedByGroup: Record<string, number>
  pending: number
}

interface ServiceOffsetState {
  produced: number
  consumed: number
  pending: number
  partitionPending: number[]
  partitionTotal: number[]
}

type ServiceOffsetMap = Record<ServiceName, ServiceOffsetState>

function emptyServiceState(): ServiceOffsetState {
  return {
    produced: 0,
    consumed: 0,
    pending: 0,
    partitionPending: Array(PARTITIONS_PER_TOPIC).fill(0),
    partitionTotal: Array(PARTITIONS_PER_TOPIC).fill(0),
  }
}

function App() {
  const [count, setCount] = useState('')
  const [messages, setMessages] = useState<Message[]>([])
  const [sending, setSending] = useState(false)
  const [wsConnected, setWsConnected] = useState(false)
  const [queueStatus, setQueueStatus] = useState<QueueStatus>({})
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [serviceOffsets, setServiceOffsets] = useState<ServiceOffsetMap>({
    orders: emptyServiceState(),
    addresses: emptyServiceState(),
    payments: emptyServiceState(),
  })
  const [metrics, setMetrics] = useState<ServiceMetricsData | null>(null)
  const [pollInterval, setPollInterval] = useState(2) // 消费者轮询间隔（秒）
  const logIdRef = useRef(0)

  const addLog = useCallback((service: string, color: string, text: string) => {
    logIdRef.current++
    const entry: LogEntry = {
      id: logIdRef.current,
      time: new Date().toLocaleTimeString(),
      service,
      color,
      text,
    }
    setLogs(prev => [...prev.slice(-200), entry])
  }, [])

  const updateServiceOffsetsFromQueue = useCallback((qs: QueueStatus) => {
    const newOffsets = { ...serviceOffsets }
    for (const svc of SERVICES) {
      const partitions = qs[svc]
      if (!partitions) continue
      const state = emptyServiceState()
      let totalPending = 0
      let totalProduced = 0
      partitions.forEach((pq: PartitionQueue, i: number) => {
        state.partitionPending[i] = pq.pending
        state.partitionTotal[i] = pq.highWatermark
        totalPending += pq.pending
        totalProduced += pq.highWatermark
      })
      state.pending = totalPending
      state.produced = totalProduced
      state.consumed = totalProduced - totalPending
      newOffsets[svc] = state
    }
    setServiceOffsets(newOffsets)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [serviceOffsets])

  const handleWsMessage = useCallback((data: unknown) => {
    const d = data as Record<string, unknown>
    if (d.type === 'message') {
      const msg = d as unknown as Message
      setMessages(prev => [...prev, msg])
      setWsConnected(true)
      const topic = msg.topic as ServiceName
      const color = SERVICE_COLORS[topic] || '#8b5cf6'
      const label = SERVICE_LABELS[topic] || topic
      addLog(label, color, `发送 ${msg.key} → ${topic}/P${msg.partition} (offset=${msg.offset})`)
      setTimeout(() => {
        addLog('Broker', '#0ea5e9', `接收 ${msg.key} → topic=${msg.topic} partition-${msg.partition} offset=${msg.offset}`)
      }, 50)
    } else if (d.type === 'queue') {
      const topics = d.topics as QueueStatus
      if (topics) {
        setQueueStatus(topics)
        updateServiceOffsetsFromQueue(topics)
      }
      setWsConnected(true)
    } else if (d.type === 'offset') {
      setWsConnected(true)
      const offsetData = d as unknown as { groupId: string; topic: string; partition: number; offset: number }
      const topic = offsetData.topic as ServiceName
      const color = SERVICE_COLORS[topic] || '#888'
      const label = SERVICE_LABELS[topic] || topic
      addLog(`${label}消费者`, color, `消费 ${topic}/P${offsetData.partition} offset → ${offsetData.offset}`)
    } else if (d.type === 'snapshot') {
      setWsConnected(true)
    }
  }, [addLog, updateServiceOffsetsFromQueue])

  useWebSocket(handleWsMessage)

  // 轮询所有 consumer 端口状态
  const consumerPorts = [
    { port: 8082, topic: 'orders' as ServiceName, p: 0 },
    { port: 8083, topic: 'orders' as ServiceName, p: 1 },
    { port: 8084, topic: 'orders' as ServiceName, p: 2 },
    { port: 8085, topic: 'addresses' as ServiceName, p: 0 },
    { port: 8086, topic: 'addresses' as ServiceName, p: 1 },
    { port: 8087, topic: 'addresses' as ServiceName, p: 2 },
    { port: 8088, topic: 'payments' as ServiceName, p: 0 },
    { port: 8089, topic: 'payments' as ServiceName, p: 1 },
    { port: 8090, topic: 'payments' as ServiceName, p: 2 },
  ]

  const prevOffsetsRef = useRef<Record<string, Record<string, number>>>({})
  useEffect(() => {
    const interval = setInterval(async () => {
      for (const cp of consumerPorts) {
        try {
          const st = await fetchConsumerStatus(cp.port)
          const groupId = st.id
          const offsets = st.offsets || {}
          const prev = prevOffsetsRef.current[groupId] || {}
          for (const [key, val] of Object.entries(offsets)) {
            const prevVal = prev[key] || 0
            if (val > prevVal) {
              const color = SERVICE_COLORS[cp.topic]
              const label = SERVICE_LABELS[cp.topic]
              addLog(`${label}消费者`, color, `消费 ${val - prevVal} 条消息 from ${key}`)
            }
          }
          prevOffsetsRef.current[groupId] = { ...offsets }
        } catch { /* consumer port not available */ }
      }
    }, 3000)
    return () => clearInterval(interval)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 轮询消费者 QPS 和文件大小指标
  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const data = await fetchConsumerMetrics()
        setMetrics(data)
      } catch { /* consumer service not available */ }
    }
    fetchMetrics()
    const interval = setInterval(fetchMetrics, 2000)
    return () => clearInterval(interval)
  }, [])

  const handleSend = async () => {
    const num = parseInt(count)
    if (isNaN(num) || num <= 0) return
    setSending(true)
    try {
      const result = await sendBatch(num)
      addLog('Frontend', '#6366f1', `发送 ${num} 条任务到 Backend API`)
      if (result.results) {
        const topicCounts: Record<string, number> = {}
        for (const r of result.results) {
          topicCounts[r.topic] = (topicCounts[r.topic] || 0) + 1
        }
        for (const [topic, cnt] of Object.entries(topicCounts)) {
          const t = topic as ServiceName
          const color = SERVICE_COLORS[t] || '#8b5cf6'
          const label = SERVICE_LABELS[t] || topic
          addLog(label, color, `转发 ${cnt} 条消息 → ${topic}`)
        }
      }
    } catch (e) {
      addLog('Frontend', '#ef4444', `发送失败: ${e}`)
    }
    setSending(false)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleSend()
  }

  // 按 topic+partition 分组消息
  const messagesByTopicPartition = new Map<string, Message[]>()
  for (const msg of messages) {
    const key = `${msg.topic}-${msg.partition}`
    if (!messagesByTopicPartition.has(key)) messagesByTopicPartition.set(key, [])
    messagesByTopicPartition.get(key)!.push(msg)
  }

  // 计算每个 topic 的统计数据
  const getServiceQueueInfo = (topic: string) => {
    const partitions = queueStatus[topic]
    if (!partitions) return { pending: 0, produced: 0, partitionPending: [0, 0, 0], partitionTotal: [0, 0, 0] }
    let pending = 0
    let produced = 0
    const partitionPending: number[] = []
    const partitionTotal: number[] = []
    partitions.forEach((pq) => {
      pending += pq.pending
      produced += pq.highWatermark
      partitionPending.push(pq.pending)
      partitionTotal.push(pq.highWatermark)
    })
    return { pending, produced, partitionPending, partitionTotal }
  }

  // 汇总统计
  let totalPending = 0
  let totalProduced = 0
  for (const svc of SERVICES) {
    const info = getServiceQueueInfo(svc)
    totalPending += info.pending
    totalProduced += info.produced
  }

  // 可复用组件：虚拟滚动的队列消息列表
  const QUEUE_MSG_HEIGHT = 20
  function QueueMessageList({ msgs, color, total, pending }: {
    msgs: Message[]
    color: string
    total: number
    pending: number
  }) {
    const parentRef = useRef<HTMLDivElement>(null)
    const virtualizer = useVirtualizer({
      count: msgs.length,
      getScrollElement: () => parentRef.current,
      estimateSize: () => QUEUE_MSG_HEIGHT,
      overscan: 5,
    })
    if (msgs.length === 0) return <div className="queue-empty">空</div>
    const consumedUpTo = total - pending
    return (
      <div ref={parentRef} className="queue-messages-virtual" style={{ height: 120, overflow: 'auto' }}>
        <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
          {virtualizer.getVirtualItems().map(vi => {
            const msg = msgs[vi.index]
            const isPending = msg.offset >= consumedUpTo
            return (
              <div
                key={vi.key}
                className={`queue-msg ${isPending ? 'pending' : 'consumed'}`}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  transform: `translateY(${vi.start}px)`,
                  height: QUEUE_MSG_HEIGHT,
                  borderLeftColor: color,
                }}
              >
                <span className="queue-msg-offset">#{msg.offset}</span>
                <span className="queue-msg-key">{msg.key}</span>
                <span className={`queue-msg-status ${isPending ? 'status-pending' : 'status-consumed'}`}>
                  {isPending ? '排队中' : '已消费'}
                </span>
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  return (
    <div className="app">
      <header className="header">
        <h1>Kafka 模拟系统</h1>
        <div className={`ws-status ${wsConnected ? 'connected' : 'disconnected'}`}>
          {wsConnected ? '● Broker 已连接' : '○ 连接中...'}
        </div>
      </header>

      {/* 架构图 */}
      <div className="card" style={{ marginBottom: 20, padding: '16px 20px' }}>
        <h2 style={{ marginBottom: 8 }}>架构图</h2>
        <ArchitectureDiagram />
      </div>

      {/* 服务总览卡片 */}
      <div className="service-cards">
        {SERVICES.map(svc => {
          const info = getServiceQueueInfo(svc)
          const consumed = info.produced - info.pending
          return (
            <div key={svc} className={`service-card ${svc}`}>
              <div className="service-card-header">
                <span className="service-card-title" style={{ color: SERVICE_COLORS[svc] }}>
                  {SERVICE_LABELS[svc]}服务 ({svc})
                </span>
                <span className="service-card-badge">{svc}-group</span>
              </div>
              <div className="service-stats">
                <div className="service-stat">
                  <div className="service-stat-value" style={{ color: SERVICE_COLORS[svc] }}>{info.produced}</div>
                  <div className="service-stat-label">已生产</div>
                </div>
                <div className="service-stat">
                  <div className="service-stat-value" style={{ color: '#10b981' }}>{consumed}</div>
                  <div className="service-stat-label">已消费</div>
                </div>
                <div className="service-stat">
                  <div className={`service-stat-value ${info.pending > 0 ? 'active-pulse' : ''}`} style={{ color: info.pending > 0 ? '#f59e0b' : SERVICE_COLORS[svc] }}>{info.pending}</div>
                  <div className="service-stat-label">排队中</div>
                </div>
              </div>
              <div className="service-partition-bars">
                {[0, 1, 2].map(pIdx => (
                  <div key={pIdx} className="service-partition-row">
                    <span className="service-partition-label">P{pIdx}</span>
                    <div className="service-partition-bar-bg">
                      <div
                        className="service-partition-bar-fill"
                        style={{
                          width: `${info.partitionTotal[pIdx] > 0 ? (info.partitionPending[pIdx] / info.partitionTotal[pIdx]) * 100 : 0}%`,
                          backgroundColor: SERVICE_COLORS[svc],
                        }}
                      />
                    </div>
                    <span className="service-partition-count">{info.partitionPending[pIdx]}</span>
                  </div>
                ))}
              </div>
            </div>
          )
        })}
      </div>

      {/* Consumer QPS 监控面板 */}
      <div className="qps-panel">
        {SERVICES.map(svc => {
          const m = metrics?.[svc]
          const qps = m?.qps ?? 0
          const qpsColor = qps > 0 ? '#10b981' : '#ef4444'
          return (
            <div key={svc} className="qps-card" style={{ borderTopColor: SERVICE_COLORS[svc] }}>
              <div className="qps-card-header">
                <span className="qps-service-name" style={{ color: SERVICE_COLORS[svc] }}>
                  {SERVICE_LABELS[svc]} ({svc})
                </span>
                <span className="qps-group-badge">{svc}-group</span>
              </div>
              <div className="qps-value-row">
                <div className="qps-metric">
                  <div className="qps-metric-value" style={{ color: qpsColor }}>{qps.toFixed(1)}</div>
                  <div className="qps-metric-label">QPS (条/秒)</div>
                </div>
                <div className="qps-metric">
                  <div className="qps-metric-value" style={{ color: '#60a5fa' }}>{m?.consumedCount ?? 0}</div>
                  <div className="qps-metric-label">累计消费</div>
                </div>
                <div className="qps-metric">
                  <div className="qps-metric-value" style={{ color: '#a78bfa' }}>{m?.fileSizeHuman ?? '0 B'}</div>
                  <div className="qps-metric-label">文件大小</div>
                </div>
              </div>
            </div>
          )
        })}
      </div>

      <div className="main-layout">
        {/* 左侧 */}
        <div className="left-panel">
          {/* 发送面板 */}
          <div className="card">
            <h2>批量发送</h2>
            <div className="hint-box">
              <p className="hint">输入发送数量 N，系统将执行 N 次迭代。每次迭代生成随机数 n∈[0,10)，按以下规则分发：</p>
              <div className="random-rules">
                <div className="rule-row"><span className="rule-num">0,1</span><span className="rule-arrow">→</span><span className="rule-target">orders（1 条 offset）</span></div>
                <div className="rule-row"><span className="rule-num">2,3</span><span className="rule-arrow">→</span><span className="rule-target">addresses（1 条 offset）</span></div>
                <div className="rule-row"><span className="rule-num">4,5</span><span className="rule-arrow">→</span><span className="rule-target">payments（1 条 offset）</span></div>
                <div className="rule-row"><span className="rule-num">6</span><span className="rule-arrow">→</span><span className="rule-target">orders + addresses（2 条 offset）</span></div>
                <div className="rule-row"><span className="rule-num">7</span><span className="rule-arrow">→</span><span className="rule-target">orders + payments（2 条 offset）</span></div>
                <div className="rule-row"><span className="rule-num">8</span><span className="rule-arrow">→</span><span className="rule-target">addresses + payments（2 条 offset）</span></div>
                <div className="rule-row"><span className="rule-num">9</span><span className="rule-arrow">→</span><span className="rule-target">全部三个（3 条 offset）</span></div>
              </div>
            </div>
            <div className="send-row">
              <input
                type="number"
                min="1"
                max="999"
                value={count}
                onChange={e => setCount(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="输入数字..."
                className="count-input"
              />
              <button
                onClick={handleSend}
                disabled={sending || !count || parseInt(count) <= 0}
                className="send-btn"
              >
                {sending ? '发送中...' : '发送'}
              </button>
            </div>
          </div>

          {/* 消费者轮询配置 */}
          <div className="card poll-config">
            <h2>⚙ 消费者轮询配置</h2>
            <p className="hint">设置 Consumer 向 Broker 拉取消息的间隔（Poll Interval）。间隔越小消费越快，但会增加 Broker 负载。</p>
            <div className="poll-row">
              <label className="poll-label">轮询间隔</label>
              <div className="poll-controls">
                <button className="poll-btn" onClick={() => setPollInterval(v => Math.max(1, v - 1))}>- 1s</button>
                <span className="poll-value">{pollInterval}s</span>
                <button className="poll-btn" onClick={() => setPollInterval(v => Math.min(10, v + 1))}>+ 1s</button>
              </div>
            </div>
            <p className="hint" style={{ marginTop: 8, fontSize: 11 }}>
              当前配置: 每 {pollInterval} 秒拉取一次，每次最多 100 条，理论上限 {Math.round(100 / pollInterval * 3)} 条/秒（3 个 partition）
            </p>
          </div>

          {/* 数据文件提示 */}
          <div className="card data-notice">
            <h2>📁 数据文件说明</h2>
            <p className="hint">消费者消费的消息会落库到以下目录的 JSON 文件：</p>
            <div className="file-paths">
              <code>kafka-sim/data/orders.json</code>
              <code>kafka-sim/data/addresses.json</code>
              <code>kafka-sim/data/payments.json</code>
            </div>
            <p className="hint warning">️ 演示结束后请手动删除这些文件，或运行清理脚本：</p>
            <code className="clean-cmd">bash kafka-sim/scripts/clean-data.sh</code>
          </div>

          {/* Broker 队列可视化 */}
          <div className="card">
            <h2>Broker 队列 <span className="msg-count">{totalPending} 条排队</span></h2>
            <div className="broker-queue-view">
              {TOPICS.map(topic => {
                const color = SERVICE_COLORS[topic as ServiceName]
                const label = SERVICE_LABELS[topic as ServiceName]
                const info = getServiceQueueInfo(topic)
                return (
                  <div key={topic} className="broker-topic-section" style={{ borderColor: color }}>
                    <div className="broker-topic-header" style={{ color }}>
                      <span>{label} ({topic})</span>
                      <span style={{ color: 'var(--text-muted)', fontWeight: 'normal' }}>
                        {info.pending} 待消费 / {info.produced} 总计
                      </span>
                    </div>
                    <div className="broker-topic-partitions">
                      {[0, 1, 2].map(pIdx => {
                        const key = `${topic}-${pIdx}`
                        const msgs = messagesByTopicPartition.get(key) || []
                        const pending = info.partitionPending[pIdx]
                        const total = info.partitionTotal[pIdx]
                        return (
                          <div key={pIdx} className="broker-topic-partition">
                            <div className="broker-partition-header">
                              <span className="broker-partition-name" style={{ color }}>P{pIdx}</span>
                              <span className="broker-partition-stats">{msgs.length} 条 / {pending} 待消费</span>
                            </div>
                            <QueueMessageList msgs={msgs} color={color} total={total} pending={pending} />
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        </div>

        {/* 右侧 */}
        <div className="right-panel">
          {/* Offset 消费面板 */}
          <div className="card">
            <h2>Offset 消费追踪</h2>
            <div className="offset-grid">
              {SERVICES.map(svc => {
                const partitions = queueStatus[svc]
                return (
                  <div key={svc} className="offset-service" style={{ borderColor: SERVICE_COLORS[svc] }}>
                    <div className="offset-service-title" style={{ color: SERVICE_COLORS[svc] }}>
                      {SERVICE_LABELS[svc]}
                    </div>
                    {partitions ? partitions.map((pq, pIdx) => (
                      <div key={pIdx} className="offset-partition-row">
                        <span>P{pIdx}</span>
                        <span>
                          HW: <span className="offset-value">{pq.highWatermark}</span>
                        </span>
                        <span>
                          待消费: <span className="offset-value" style={{ color: pq.pending > 0 ? '#f59e0b' : '#10b981' }}>{pq.pending}</span>
                        </span>
                      </div>
                    )) : (
                      <div className="offset-partition-row">
                        <span style={{ color: 'var(--text-muted)' }}>等待数据...</span>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>

          {/* 服务日志 */}
          <div className="card">
            <h2>服务日志 <span className="msg-count">{logs.length} 条</span></h2>
            <LogVirtualList logs={logs} />
          </div>

          {/* 消息记录 */}
          <div className="card">
            <h2>消息记录 <span className="msg-count">{messages.length} 条</span></h2>
            <MessageVirtualList messages={messages} />
          </div>
        </div>
      </div>
    </div>
  )
}

// 虚拟滚动：服务日志列表
const LOG_ITEM_HEIGHT = 26
function LogVirtualList({ logs }: { logs: LogEntry[] }) {
  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: logs.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => LOG_ITEM_HEIGHT,
    overscan: 5,
  })
  const reversed = [...logs].reverse()
  if (reversed.length === 0) {
    return <div className="log-list-empty">等待事件...发送任务后将显示各服务的实时日志</div>
  }
  return (
    <div ref={parentRef} className="log-list-virtual" style={{ height: 300, overflow: 'auto' }}>
      <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
        {virtualizer.getVirtualItems().map(vi => {
          const entry = reversed[vi.index]
          return (
            <div
              key={entry.id}
              className="log-entry"
              style={{
                position: 'absolute',
                top: 0,
                left: 0,
                width: '100%',
                transform: `translateY(${vi.start}px)`,
                height: LOG_ITEM_HEIGHT,
              }}
            >
              <span className="log-time">{entry.time}</span>
              <span className="log-service" style={{ color: entry.color, borderColor: entry.color }}>
                {entry.service}
              </span>
              <span className="log-text">{entry.text}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// 虚拟滚动：消息记录列表
const MSG_ITEM_HEIGHT = 80
function MessageVirtualList({ messages }: { messages: Message[] }) {
  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => MSG_ITEM_HEIGHT,
    overscan: 5,
  })
  const reversed = [...messages].reverse()
  if (reversed.length === 0) {
    return <div className="message-list-empty">等待消息...发送任务后，所有消息记录将显示在这里</div>
  }
  return (
    <div ref={parentRef} className="message-list-virtual" style={{ height: 500, overflow: 'auto' }}>
      <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
        {virtualizer.getVirtualItems().map(vi => {
          const msg = reversed[vi.index]
          const topic = msg.topic as ServiceName
          const color = SERVICE_COLORS[topic] || '#888'
          const label = SERVICE_LABELS[topic] || msg.topic
          return (
            <div
              key={vi.key}
              className="message-item"
              style={{
                position: 'absolute',
                top: 0,
                left: 0,
                width: 'calc(100% - 0px)',
                transform: `translateY(${vi.start}px)`,
                height: MSG_ITEM_HEIGHT,
                borderColor: 'var(--border)',
              }}
            >
              <div className="msg-header">
                <span className="service-badge" style={{ backgroundColor: color }}>
                  {label}
                </span>
                <span className="msg-meta">{msg.topic} P{msg.partition} · Offset {msg.offset}</span>
              </div>
              <div className="msg-body">
                <span className="msg-key">{msg.key}</span>
                <span className="msg-value">{msg.value}</span>
              </div>
              <div className="msg-time">{new Date(msg.timestamp).toLocaleTimeString()}</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default App

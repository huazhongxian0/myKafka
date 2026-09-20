export function ArchitectureDiagram() {
  const serviceColors = {
    orders: '#3b82f6',
    addresses: '#10b981',
    payments: '#f59e0b',
  }

  const serviceLabels = {
    orders: '订单',
    addresses: '地址',
    payments: '支付',
  }

  const servicePorts = {
    orders: [8082, 8083, 8084],
    addresses: [8085, 8086, 8087],
    payments: [8088, 8089, 8090],
  }

  const topics = ['orders', 'addresses', 'payments'] as const

  // 上方为消息流，下方为说明卡片；统一服务行距，避免跨区重叠。
  const W = 1180, H = 470
  const serviceTop = 20
  const serviceRowGap = 84
  const notesOffsetY = 70

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', display: 'block' }}>
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <path d="M0,0 L8,3 L0,6" fill="#475569" />
        </marker>
        <marker id="arrow-dash" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <path d="M0,0 L8,3 L0,6" fill="#334155" />
        </marker>
      </defs>

      {/* 背景 */}
      <rect width={W} height={H} rx="12" fill="#0f172a" />

      {/* 网格 */}
      {Array.from({ length: Math.ceil(W / 30) }, (_, i) => (
        <line key={`v${i}`} x1={i * 30} y1="0" x2={i * 30} y2={H} stroke="#1e293b" strokeWidth="0.5" />
      ))}
      {Array.from({ length: Math.ceil(H / 30) }, (_, i) => (
        <line key={`h${i}`} x1="0" y1={i * 30} x2={W} y2={i * 30} stroke="#1e293b" strokeWidth="0.5" />
      ))}

      {/* === Column 1: Frontend + Backend (x: 20-170) === */}
      {/* Frontend */}
      <rect x="25" y="30" width="140" height="36" rx="8" fill="#6366f1" />
      <text x="95" y="47" textAnchor="middle" fill="#fff" fontSize="13" fontWeight="bold" fontFamily="monospace">Frontend</text>
      <text x="95" y="60" textAnchor="middle" fill="rgba(255,255,255,0.6)" fontSize="9" fontFamily="monospace">:3000 React</text>

      {/* Frontend -> Backend (垂直短箭头) */}
      <line x1="95" y1="66" x2="95" y2="82" stroke="#475569" strokeWidth="1.5" markerEnd="url(#arrow)" />
      <text x="112" y="77" fill="#64748b" fontSize="8" fontFamily="monospace">HTTP</text>

      {/* Backend API */}
      <rect x="25" y="84" width="140" height="36" rx="8" fill="#8b5cf6" />
      <text x="95" y="101" textAnchor="middle" fill="#fff" fontSize="13" fontWeight="bold" fontFamily="monospace">Backend API</text>
      <text x="95" y="114" textAnchor="middle" fill="rgba(255,255,255,0.6)" fontSize="9" fontFamily="monospace">:8080 Producer</text>

      {/* Backend -> Broker (水平箭头) */}
      <line x1="165" y1="102" x2="260" y2="102" stroke="#475569" strokeWidth="1.5" markerEnd="url(#arrow)" />
      <text x="190" y="94" fill="#64748b" fontSize="8" fontFamily="monospace">Produce</text>

      {/* === Column 2: Broker (x: 260-620) === */}
      <rect x="265" y="15" width="360" height="175" rx="12" fill="#0c1425" stroke="#0ea5e9" strokeWidth="1.5" />
      <text x="445" y="36" textAnchor="middle" fill="#0ea5e9" fontSize="14" fontWeight="bold" fontFamily="monospace">Broker :8081</text>

      {/* Topic rows inside broker */}
      {topics.map((topic, tIdx) => {
        const y = 50 + tIdx * 42
        return (
          <g key={topic}>
            <text x="282" y={y + 24} fill={serviceColors[topic]} fontSize="12" fontWeight="bold" fontFamily="monospace">
              {topic}
            </text>
            {[0, 1, 2].map(pIdx => {
              const px = 365 + pIdx * 80
              return (
                <g key={pIdx}>
                  <rect x={px} y={y + 6} width="70" height="26" rx="4" fill="rgba(255,255,255,0.05)" stroke={serviceColors[topic]} strokeWidth="0.5" strokeDasharray="2 2" />
                  <text x={px + 35} y={y + 23} textAnchor="middle" fill="#94a3b8" fontSize="10" fontFamily="monospace">P{pIdx}</text>
                </g>
              )
            })}
          </g>
        )
      })}

      {/* === Poll 连接线: Broker → Consumer (水平直线) === */}
      {topics.map((topic, tIdx) => {
        const brokerRight = 625
        const topicY = 50 + tIdx * 42 + 19 // topic 行中心
        const consumerLeft = 710
        const consumerY = serviceTop + tIdx * serviceRowGap + 32 // consumer 行中心
        return (
          <g key={`line-${topic}`}>
            <line
              x1={brokerRight} y1={topicY}
              x2={consumerLeft} y2={consumerY}
              stroke="#334155" strokeWidth="1.5" strokeDasharray="4 4" markerEnd="url(#arrow-dash)"
            />
            <text x={(brokerRight + consumerLeft) / 2 - 12} y={(topicY + consumerY) / 2 - 6} fill="#64748b" fontSize="8" fontFamily="monospace">Poll</text>
          </g>
        )
      })}

      {/* === Column 3: Consumer Services (x: 710-910) === */}
      {topics.map((topic, tIdx) => {
        const cx = 710
        const cy = serviceTop + tIdx * serviceRowGap
        const color = serviceColors[topic]
        const label = serviceLabels[topic]
        const ports = servicePorts[topic]
        return (
          <g key={`svc-${topic}`}>
            <rect x={cx} y={cy} width="195" height="64" rx="8" fill={color} fillOpacity="0.15" stroke={color} strokeWidth="1.5" />
            <text x={cx + 97} y={cy + 22} textAnchor="middle" fill={color} fontSize="13" fontWeight="bold" fontFamily="monospace">
              {label}服务 ({topic})
            </text>
            {ports.map((port, pIdx) => (
              <text key={pIdx} x={cx + 22 + pIdx * 60} y={cy + 40} textAnchor="middle" fill="#94a3b8" fontSize="8" fontFamily="monospace">
                :{port} P{pIdx}
              </text>
            ))}
            <text x={cx + 97} y={cy + 56} textAnchor="middle" fill="#64748b" fontSize="8" fontFamily="monospace">
              groupId: {topic}-group
            </text>
          </g>
        )
      })}

      {/* === Column 4: DBs (x: 940-1050), 水平箭头从 Consumer 指向 DB === */}
      {topics.map((topic, tIdx) => {
        const consumerRight = 905
        const cy = serviceTop + tIdx * serviceRowGap
        const color = serviceColors[topic]
        const label = serviceLabels[topic]
        const dbX = 940
        const dbCy = cy + 32
        return (
          <g key={`db-${topic}`}>
            {/* 水平箭头 Consumer → DB */}
            <line x1={consumerRight} y1={dbCy} x2={dbX - 2} y2={dbCy} stroke="#475569" strokeWidth="1" markerEnd="url(#arrow)" />
            {/* DB 圆柱 */}
            <ellipse cx={dbX + 50} cy={dbCy + 14} rx="45" ry="12" fill="none" stroke={color} strokeWidth="1" opacity="0.5" />
            <rect x={dbX + 5} y={dbCy + 2} width="90" height="24" rx="0" fill="none" stroke={color} strokeWidth="1" opacity="0.5" />
            <ellipse cx={dbX + 50} cy={dbCy + 2} rx="45" ry="12" fill={color} fillOpacity="0.1" stroke={color} strokeWidth="1" />
            <text x={dbX + 50} y={dbCy + 7} textAnchor="middle" fill={color} fontSize="10" fontWeight="bold" fontFamily="monospace">
              {label} DB
            </text>
          </g>
        )
      })}

      {/* === 随机数规则表 (底部 x: 20-500, y: 210-380) === */}
      <g transform={`translate(0, ${notesOffsetY})`}>
        <rect x="25" y="210" width="360" height="170" rx="10" fill="rgba(255,255,255,0.03)" stroke="#334155" strokeWidth="0.5" />
        <text x="40" y="232" fill="#e2e8f0" fontSize="12" fontWeight="bold" fontFamily="monospace"> 随机数分发规则</text>
        <text x="40" y="248" fill="#94a3b8" fontSize="9" fontFamily="monospace">每次迭代生成随机数 n∈[0,10)：</text>

        {/* Rule rows */}
        {[
          { nums: '0,1', target: 'orders', desc: '1 条 offset', color: '#3b82f6' },
          { nums: '2,3', target: 'addresses', desc: '1 条 offset', color: '#10b981' },
          { nums: '4,5', target: 'payments', desc: '1 条 offset', color: '#f59e0b' },
          { nums: '6', target: 'orders + addresses', desc: '2 条 offset', color: '#3b82f6' },
          { nums: '7', target: 'orders + payments', desc: '2 条 offset', color: '#3b82f6' },
          { nums: '8', target: 'addresses + payments', desc: '2 条 offset', color: '#10b981' },
          { nums: '9', target: '全部三个', desc: '3 条 offset', color: '#8b5cf6' },
        ].map((rule, i) => {
          const ry = 266 + i * 14
          return (
            <g key={i}>
              <rect x="40" y={ry - 9} width="26" height="13" rx="3" fill={rule.color} opacity="0.8" />
              <text x="53" y={ry + 1} textAnchor="middle" fill="#fff" fontSize="8" fontWeight="bold" fontFamily="monospace">{rule.nums}</text>
              <text x="74" y={ry + 1} fill="#64748b" fontSize="8" fontFamily="monospace">→</text>
              <text x="90" y={ry + 1} fill="#e2e8f0" fontSize="8" fontFamily="monospace">{rule.target}</text>
              <text x="240" y={ry + 1} fill="#64748b" fontSize="8" fontFamily="monospace">({rule.desc})</text>
            </g>
          )
        })}
      </g>

      {/* === 数据文件说明 (底部 x: 410-700, y: 210-380) === */}
      <g transform={`translate(0, ${notesOffsetY})`}>
        <rect x="410" y="210" width="360" height="170" rx="10" fill="rgba(255,255,255,0.03)" stroke="#f59e0b" strokeWidth="0.5" />
        <text x="425" y="232" fill="#e2e8f0" fontSize="12" fontWeight="bold" fontFamily="monospace">📁 数据文件说明</text>
        <text x="425" y="250" fill="#94a3b8" fontSize="9" fontFamily="monospace">消费消息落库到：</text>
        <text x="425" y="268" fill="#e2e8f0" fontSize="9" fontFamily="monospace">kafka-sim/data/orders.json</text>
        <text x="425" y="284" fill="#e2e8f0" fontSize="9" fontFamily="monospace">kafka-sim/data/addresses.json</text>
        <text x="425" y="300" fill="#e2e8f0" fontSize="9" fontFamily="monospace">kafka-sim/data/payments.json</text>
        <text x="425" y="324" fill="#f59e0b" fontSize="9" fontFamily="monospace">演示结束后请手动删除，或运行：</text>
        <text x="425" y="342" fill="#10b981" fontSize="9" fontFamily="monospace">bash kafka-sim/scripts/clean-data.sh</text>
      </g>

      {/* === 轮询配置说明 (底部 x: 795-1080, y: 210-380) === */}
      <g transform={`translate(0, ${notesOffsetY})`}>
        <rect x="795" y="210" width="360" height="170" rx="10" fill="rgba(255,255,255,0.03)" stroke="#6366f1" strokeWidth="0.5" />
        <text x="810" y="232" fill="#e2e8f0" fontSize="12" fontWeight="bold" fontFamily="monospace">⚙ Consumer 轮询机制</text>
        <text x="810" y="250" fill="#94a3b8" fontSize="9" fontFamily="monospace">每个 PartitionListener 独立轮询 Broker：</text>
        <text x="810" y="270" fill="#94a3b8" fontSize="9" fontFamily="monospace">1. 发送 Fetch 请求 (from offset)</text>
        <text x="810" y="286" fill="#94a3b8" fontSize="9" fontFamily="monospace">2. 收到消息 → 落库 + 更新内存</text>
        <text x="810" y="302" fill="#94a3b8" fontSize="9" fontFamily="monospace">3. Commit Offset 到 Broker</text>
        <text x="810" y="322" fill="#94a3b8" fontSize="9" fontFamily="monospace">默认间隔 2 秒，每次最多 100 条</text>
        <text x="810" y="342" fill="#94a3b8" fontSize="9" fontFamily="monospace">9 个端口 × 50 条/秒 = 理论 450 条/秒</text>
        <text x="810" y="362" fill="#64748b" fontSize="8" fontFamily="monospace">间隔越小消费越快，但 Broker 负载越大</text>
      </g>
    </svg>
  )
}

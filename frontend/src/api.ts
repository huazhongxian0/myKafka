import { useEffect, useRef, useCallback } from 'react'

const BROKER_WS = '/ws'
const API_BASE = '/api'

export function useWebSocket(onMessage: (data: unknown) => void) {
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>(undefined)

  const connect = useCallback(() => {
    try {
      const ws = new WebSocket(BROKER_WS)
      wsRef.current = ws

      ws.onopen = () => {
        console.log('[WS] Connected to broker')
      }

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data) as Record<string, unknown>
          onMessage(data)
        } catch (e) {
          console.error('[WS] Parse error:', e)
        }
      }

      ws.onclose = () => {
        console.log('[WS] Disconnected, reconnecting in 3s...')
        reconnectTimer.current = setTimeout(connect, 3000)
      }

      ws.onerror = (err) => {
        console.error('[WS] Error:', err)
        ws.close()
      }
    } catch (e) {
      console.error('[WS] Connect error:', e)
      reconnectTimer.current = setTimeout(connect, 3000)
    }
  }, [onMessage])

  useEffect(() => {
    connect()
    return () => {
      clearTimeout(reconnectTimer.current)
      wsRef.current?.close()
    }
  }, [connect])
}

export async function sendBatch(count: number) {
  const resp = await fetch(`${API_BASE}/send/batch`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ count }),
  })
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
  return resp.json() as Promise<import('./types').BatchResult>
}

export async function fetchServices() {
  const resp = await fetch(`${API_BASE}/services`)
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
  return resp.json() as Promise<import('./types').ServiceInfo[]>
}

export async function fetchConsumerStatus(port: number) {
  const proxyPath = `/consumer-port-${port}`
  const resp = await fetch(`${proxyPath}/status`)
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
  return resp.json() as Promise<{
    id: string
    offsets: Record<string, number>
    port: number
    topic: string
    partition: number
  }>
}

export async function fetchConsumerMetrics() {
  const resp = await fetch(`${API_BASE}/consumer/metrics`)
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
  return resp.json() as Promise<import('./types').ServiceMetricsData>
}

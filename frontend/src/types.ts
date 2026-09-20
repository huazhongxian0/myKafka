export type ServiceName = 'orders' | 'addresses' | 'payments'

export const SERVICE_COLORS: Record<ServiceName, string> = {
  orders: '#3b82f6',
  addresses: '#10b981',
  payments: '#f59e0b',
}

export const SERVICE_LABELS: Record<ServiceName, string> = {
  orders: '订单',
  addresses: '地址',
  payments: '支付',
}

export const SERVICES: ServiceName[] = ['orders', 'addresses', 'payments']

export const TOPICS = ['orders', 'addresses', 'payments'] as const
export const PARTITIONS_PER_TOPIC = 3

export interface Message {
  type: string
  topic: string
  partition: number
  offset: number
  key: string
  value: string
  timestamp: number
}

export interface OffsetEvent {
  type: string
  groupId: string
  topic: string
  partition: number
  offset: number
}

export interface ConsumerPortInfo {
  port: number
  partition: number
}

export interface ServiceInfo {
  name: ServiceName
  topic: string
  groupId: string
  ports: ConsumerPortInfo[]
}

export interface BatchResult {
  total: number
  results: Array<{
    topic: string
    partition: number
    offset: number
    key: string
  }>
}

export interface PartitionQueue {
  highWatermark: number
  committedByGroup: Record<string, number>
  pending: number
}

export type QueueStatus = Record<string, PartitionQueue[]>

export interface ConsumerOffsets {
  [groupId: string]: { [key: string]: number }
}

export interface ServiceMetricsData {
  [service: string]: {
    qps: number
    consumedCount: number
    fileSize: number
    fileSizeHuman: string
  }
}

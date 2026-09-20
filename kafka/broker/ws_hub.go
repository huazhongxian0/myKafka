package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSClient WebSocket 客户端连接
type WSClient struct {
	conn *websocket.Conn
	send chan []byte
}

// WSHub WebSocket 连接管理中心
// 负责管理与前端的所有 WebSocket 连接，广播实时事件
// 支持的事件类型: "message", "queue", "offset", "snapshot"
type WSHub struct {
	mu         sync.RWMutex
	clients    map[*WSClient]bool
	broadcast  chan []byte
	register   chan *WSClient
	unregister chan *WSClient
}

// NewWSHub 创建一个新的 WebSocket Hub
func NewWSHub() *WSHub {
	return &WSHub{
		clients:    make(map[*WSClient]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *WSClient),
		unregister: make(chan *WSClient),
	}
}

// Run 启动 Hub 的主循环，处理客户端注册、注销和消息广播
func (h *WSHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			fmt.Printf("[Broker] WebSocket client connected (total: %d)\n", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			fmt.Printf("[Broker] WebSocket client disconnected (total: %d)\n", len(h.clients))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// 客户端发送缓冲区满了，断开连接
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastJSON 将任意对象序列化为 JSON 并广播给所有连接的客户端
// 非阻塞操作：如果广播 channel 已满，消息会被丢弃
func (h *WSHub) BroadcastJSON(v interface{}) {
	data, _ := json.Marshal(v)
	select {
	case h.broadcast <- data:
	default:
		// 非阻塞：如果 channel 满了就丢弃
		fmt.Println("[Broker] WARNING: broadcast channel full, dropping message")
	}
}

// WebSocketUpgrader 用于升级 HTTP 连接到 WebSocket
var WebSocketUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// HandleWebSocket 处理 WebSocket 连接请求
// 连接建立后发送当前状态快照，然后启动读写协程
func (h *WSHub) HandleWebSocket(w http.ResponseWriter, r *http.Request, getSnapshot func() []byte) {
	conn, err := WebSocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("[Broker] WebSocket upgrade error: %v\n", err)
		return
	}

	client := &WSClient{conn: conn, send: make(chan []byte, 256)}
	h.register <- client

	// 发送当前状态快照
	if getSnapshot != nil {
		snapshot := getSnapshot()
		if snapshot != nil {
			client.send <- snapshot
		}
	}

	// 写协程：将消息写入 WebSocket 连接
	go func() {
		defer func() {
			h.unregister <- client
			conn.Close()
		}()
		for msg := range client.send {
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}()

	// 读协程：保持连接，处理 ping/pong
	go func() {
		defer func() {
			h.unregister <- client
			conn.Close()
		}()
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

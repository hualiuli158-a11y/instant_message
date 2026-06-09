package main

import "encoding/json"

// Message 定义了前后端交互的标准数据包格式
type Message struct {
	From string `json:"from"` // 发送者 ID
	To   string `json:"to"`   // 接收者 ID (如果是群聊，可以约定为 "all" 或群组 ID)
	Data string `json:"data"` // 实际的消息内容
}

// Hub 维护所有活跃的客户端，并向他们广播消息
type Hub struct {
	// 核心升级：用 UserID 作为 Key 映射到具体的 Client
	clients map[string]*Client

	// 收发通道
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[string]*Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			// 客户端上线，登记到名册里
			h.clients[client.UserID] = client

		case client := <-h.unregister:
			if _, ok := h.clients[client.UserID]; ok {
				delete(h.clients, client.UserID)
				close(client.send)
			}

		case messageData := <-h.broadcast:
			// 收到前端发来的消息，先解析 JSON 拆包
			var msg Message
			err := json.Unmarshal(messageData, &msg)
			if err != nil {
				continue // 格式不对，直接丢弃
			}

			// 路由逻辑：精准打击
			if msg.To == "all" {
				// 群发逻辑
				for _, client := range h.clients {
					select {
					case client.send <- messageData:
					default:
						close(client.send)
						delete(h.clients, client.UserID)
					}
				}
			} else {
				// 私聊逻辑：在 map 中精确查找接收方 (比如李四)
				if targetClient, ok := h.clients[msg.To]; ok {
					select {
					case targetClient.send <- messageData:
						// 成功塞入李四的专属 Channel，由李四的 writePump 单线程序列化发出，绝对线程安全！
					default:
						// 李四的缓冲区满了 (可能网卡了)，踢掉他
						close(targetClient.send)
						delete(h.clients, targetClient.UserID)
					}
				}
			}
		}
	}
}

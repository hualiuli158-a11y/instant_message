package main

// Hub 维护所有活跃的客户端，并向他们广播消息
type Hub struct {
	// 注册了的客户端 (类似你提到的全局 map)
	clients map[*Client]bool

	// 从客户端接收并需要广播出去的消息
	broadcast chan []byte

	// 客户端注册请求 (上线)
	register chan *Client

	// 客户端注销请求 (下线)
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

// Run 启动 Hub 的事件循环 (独占一个 Goroutine)
func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send) // 关闭该客户端的发送通道
			}
		case message := <-h.broadcast:
			// 收到消息，遍历所有客户端并推入他们的 send channel
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// 如果发送缓冲满了 (比如客户端卡死)，踢掉死链接
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

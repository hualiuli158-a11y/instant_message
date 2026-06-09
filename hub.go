package main

import (
	"encoding/json"
	"log"
)

const MaxClients = 1000

type Hub struct {
	clients    map[string]*Client
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
			if len(h.clients) >= MaxClients {
				limitMsg, _ := json.Marshal(Message{Type: "system", Data: "服务器已满"})
				client.send <- limitMsg
				close(client.send)
				continue
			}
			h.clients[client.UserID] = client
			h.broadcastSystemMessage(client.UserID + " 加入了群聊")
			h.broadcastOnlineUsers()

		case client := <-h.unregister:
			if _, ok := h.clients[client.UserID]; ok {
				delete(h.clients, client.UserID)
				close(client.send)
				h.broadcastSystemMessage(client.UserID + " 离开了群聊")
				h.broadcastOnlineUsers()
			}

		case messageData := <-h.broadcast:
			var msg Message
			if err := json.Unmarshal(messageData, &msg); err != nil {
				log.Printf("Hub 广播解析 JSON 失败: %v", err)
				continue
			}

			if msg.To == "all" {
				// 1. 群发分支：安全写保护
				for _, client := range h.clients {
					select {
					case client.send <- messageData:
					default:
						close(client.send)
						delete(h.clients, client.UserID)
					}
				}
			} else {
				// 2. 🔴 严重缺陷修复：私聊分支加入写保护，防止阻塞全局 Hub
				if targetClient, ok := h.clients[msg.To]; ok {
					select {
					case targetClient.send <- messageData:
					default:
						log.Printf("目标用户 %s 缓冲区满，强制下线", targetClient.UserID)
						close(targetClient.send)
						delete(h.clients, targetClient.UserID)
					}
				}

				// 3. 🟡 中等缺陷修复：给自己推消息的 default 分支补全清理逻辑
				if selfClient, ok := h.clients[msg.From]; ok {
					select {
					case selfClient.send <- messageData:
					default:
						log.Printf("发送者用户 %s 缓冲区满，强制下线", selfClient.UserID)
						close(selfClient.send)
						delete(h.clients, selfClient.UserID)
					}
				}
			}
		}
	}
}

func (h *Hub) broadcastSystemMessage(text string) {
	msg := Message{Type: "system", Data: text}
	b, _ := json.Marshal(msg)
	for _, client := range h.clients {
		select {
		case client.send <- b:
		default:
			close(client.send)
			delete(h.clients, client.UserID)
		}
	}
}

func (h *Hub) broadcastOnlineUsers() {
	var users []string
	for userID := range h.clients {
		users = append(users, userID)
	}
	msg := Message{Type: "users", UserList: users}
	b, _ := json.Marshal(msg)
	for _, client := range h.clients {
		select {
		case client.send <- b:
		default:
			close(client.send)
			delete(h.clients, client.UserID)
		}
	}
}

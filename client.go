package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// 允许写操作的最长时间（通常 10 秒足够把消息推给内核的 TCP 缓冲区了）
	writeWait = 10 * time.Second
	// 允许客户端多久不理我们（也就是读超时时间），通常设为 60 秒
	pongWait = 60 * time.Second

	// 定时向客户端发 Ping 的周期。必须比 pongWait 小！一般取 90%
	pingPeriod = (pongWait * 9) / 10

	// 限制最大的消息大小，防止恶意客户端发巨量数据撑爆内存
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// 解决跨域问题
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	hub *Hub
	// 底层的 websocket 连接
	conn *websocket.Conn
	// 缓冲 channel，用于存放要发送给该客户端的消息
	send chan []byte
}

// readPump 负责从 WebSocket 读取消息并送到 Hub
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	// 限制消息大小，防止 OOM
	c.conn.SetReadLimit(maxMessageSize)
	// 设置首次读超时时间：从现在起 60 秒内必须收到消息或 Pong
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	// 关键！注册 Pong 处理器：每次收到浏览器的 Pong 帧，就把倒计时再往后推 60 秒（续命）
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			// 一旦读超时（或者客户端真断开了），ReadMessage 会返回 err，直接 break 退出循环，回收资源
			break
		}
		c.hub.broadcast <- message
	}
}

// writePump 负责把 Client.send 里的消息推给前端
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			// 1. 发送常规消息（或关闭指令）前，先设定写超时！
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub 关闭了 channel，说明当前客户端该滚蛋了
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// 真正执行写操作。如果网络卡死，最多卡 10 秒就会报错返回 err
			err := c.conn.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				return // 写入失败（超时或连接已断），直接 break 退出协程，回收资源
			}

		case <-ticker.C:
			// 2. 发送 Ping 帧前，也要设定写超时！
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return // Ping 都发不出去，说明底层的 TCP 管道彻底废了，直接退出
			}
		}
	}
}

package main

import (
	"log"
	"net/http"
)

// serveWs 处理 WebSocket 请求
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := &Client{hub: hub, conn: conn, send: make(chan []byte, 256)}
	client.hub.register <- client

	// 启动读写协程
	go client.writePump()
	go client.readPump()
}

func main() {
	hub := newHub()
	go hub.run() // 启动全局调度器

	// 处理 WebSocket 请求
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	// 【新增这行】处理普通网页请求，把 index.html 返回给前端
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	log.Println("服务器启动在 :9999 端口...")
	err := http.ListenAndServe(":9999", nil) // 这里改成 8888
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

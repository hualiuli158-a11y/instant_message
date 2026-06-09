package main

import (
	"log"
	"math/rand"
	"net/http"
	"strconv"
)

// 🟡 中等缺陷修复：改为严格的指针传递，语义清晰，防止后续值拷贝踩坑
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	// 🟢 轻量优化：直接使用新版全局随机，彻底拿掉废弃的 rand.Seed()
	mockUserID := "User_" + strconv.Itoa(rand.Intn(10000))

	client := &Client{
		hub:    hub,
		UserID: mockUserID,
		conn:   conn,
		send:   make(chan []byte, 256),
	}
	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func main() {
	hub := newHub()
	go hub.run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	log.Println("服务器启动在 :9999 端口...")
	err := http.ListenAndServe(":9999", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

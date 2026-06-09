package main

// Message 定义了整个 IM 系统前后端交互的统一数据包规范
type Message struct {
	Type     string   `json:"type"`                // 消息类型: "chat"(聊天), "system"(系统提示), "users"(在线列表)
	From     string   `json:"from,omitempty"`      // 发送者 ID
	To       string   `json:"to,omitempty"`        // 接收者 ID ("all" 代表群发，或者具体用户的 ID)
	Data     string   `json:"data"`                // 真实的聊天文本内容
	UserList []string `json:"user_list,omitempty"` // 在线用户名单 (仅在 type="users" 时有用)
}

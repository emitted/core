package client

import (
	"encoding/json"
	"math/rand"

	"github.com/gorilla/websocket"
	p "github.com/sireax/Emmet-Go-Server/internal/packet"
)

// Client ...
type Client struct {
	uuid uint32
	Conn *websocket.Conn
}

// NewClient ...
func NewClient(conn *websocket.Conn) *Client {
	return &Client{
		uuid: rand.Uint32(),
		Conn: conn,
	}
}

// SendAuthMessage sends authentication confirmation message
func (c *Client) SendAuthMessage() {
	data, err := json.Marshal("authentication succeded")
	if err != nil {
		return
	}

	c.Conn.WriteJSON(data)
}

// SendMessage ...
func (c *Client) SendMessage(packet *p.Packet) {
	c.Conn.WriteJSON(packet)
}

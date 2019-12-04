package main

import (
	// "encoding/json"

	"log"
	"math/rand"

	"github.com/gorilla/websocket"
)

// Client ...
type Client struct {
	uid           uint32
	Conn          *websocket.Conn
	User          string
	Authenticated bool
	Subs          map[string]*Tunnel
	Stream        chan *Packet
	Closed        chan struct{}
	MessageWriter *writer
}

// NewClient ...
func NewClient(conn *websocket.Conn, tunnel *Tunnel) *Client {

	client := &Client{
		uid:           rand.Uint32(),
		Conn:          conn,
		Authenticated: true,
		Subs:          make(map[string]*Tunnel),
		Stream:        make(chan *Packet),
		Closed:        make(chan struct{}, 1),
	}

	// Creating message writer config
	messageWriterConf := writerConfig{
		WriteFn: func(data []byte) error {
			client.Write(data)
			return nil
		},
		WriteManyFn: func(data ...[]byte) error {
			for _, payload := range data {
				client.Write(payload)
			}
			return nil
		},
	}

	// Setting client's message writer
	client.MessageWriter = newWriter(messageWriterConf)
	log.Println(client.MessageWriter)

	return client
}

// Connect ...
func (c *Client) Connect(tunnel *Tunnel) {
	c.Subs[tunnel.Key] = tunnel

	first, err := hub.AddSub(tunnel)
	if err != nil {
		log.Fatal(err)
	}

	hub.Subs[tunnel.Key].Clients[c.uid] = c
	hub.Clients[c.uid] = c

	if first {
		broker.Subscribe([]string{tunnel.Key})
		err = broker.redis.Ping("ping")
		if err != nil {
			log.Fatal(err)
		}
	}

	c.WriteAuthPacket()
}

// Tunnels ...
func (c *Client) Tunnels() []string {
	var slice []string
	for t := range c.Subs {
		slice = append(slice, t)
	}

	return slice
}

// Terminate function deletes all records about client and destroys connection.
func (c *Client) Terminate() {
	c.Conn.Close()
	close(c.Closed)
	broker.Unsubscribe(c.Tunnels())
	c.MessageWriter.close()
	delete(hub.Clients, c.uid)
}

// Write ...
func (c *Client) Write(data []byte) {
	err := c.Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		log.Fatal(err)
	}
}

// WriteAuthPacket sends authentication confirmation message
func (c *Client) WriteAuthPacket() {
	packet := &Packet{
		Event: "message",
		Data:  []byte("Authentication succeded"),
	}

	c.MessageWriter.enqueue(packet.Encode())
}

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
	}

	c.WriteAuthPacket()
}

// Disconnect ...
func (c *Client) Disconnect() {
	var toDelete []string
	for _, tunnel := range c.Subs {
		delete(tunnel.Clients, c.uid)
		if tunnel.ClientsConnected() == 0 {
			delete(hub.Subs, tunnel.Key)
			toDelete = append(toDelete, tunnel.Key)
		}
	}
	broker.Unsubscribe(toDelete)
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
	log.Println("Terminate function called")
	c.Disconnect()
	c.MessageWriter.close()
	delete(hub.Clients, c.uid)
	c.Conn.Close()
}

// Write ...
func (c *Client) Write(data []byte) {
	err := c.Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		log.Fatal("error while writing message to client: ", err)
	}
}

// WriteAuthPacket sends authentication confirmation message
func (c *Client) WriteAuthPacket() {
	packet := &Packet{
		Event: "message",
		Data:  "Authentication succeded",
	}

	c.MessageWriter.enqueue(packet.Encode())
}

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
	Transport     *websocketTransport
}

// NewClient ...
func NewClient(transport *websocketTransport) *Client {

	client := &Client{
		uid:           rand.Uint32(),
		Authenticated: true,
		Subs:          make(map[string]*Tunnel),
		Stream:        make(chan *Packet),
		Transport:     transport,
	}

	// Creating message writer config
	messageWriterConf := writerConfig{
		WriteFn: func(data []byte) error {
			client.Transport.Write(data)
			return nil
		},
		WriteManyFn: func(data ...[]byte) error {
			for _, payload := range data {
				client.Transport.Write(payload)
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

// Close ...
func (c *Client) Close() {

	var toDelete []string
	for _, tunnel := range c.Subs {
		delete(tunnel.Clients, c.uid)
		if tunnel.ClientsConnected() == 0 {
			delete(hub.Subs, tunnel.Key)
			toDelete = append(toDelete, tunnel.Key)
		}
	}
	broker.Unsubscribe(toDelete)

	c.MessageWriter.close()
	delete(hub.Clients, c.uid)

	c.Transport.Close(DisconnectNormal)
}

// Terminate function deletes all records about client and destroys connection.
func (c *Client) Terminate() {
	c.Disconnect()
	c.MessageWriter.close()
	delete(hub.Clients, c.uid)
	c.Transport.Close(DisconnectNormal)
}

// WriteAuthPacket sends authentication confirmation message
func (c *Client) WriteAuthPacket() {
	packet := &Packet{
		Event: "message",
		Data:  "Authentication succeded",
	}

	c.MessageWriter.enqueue(packet.Encode())
}

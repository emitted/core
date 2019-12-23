package main

import (
	// "encoding/json"

	"encoding/json"
	"log"
	"math/rand"
)

// Client ...
type Client struct {
	uid           uint32
	Authenticated bool
	App           *App
	Subs          map[string]*Channel
	messageCh     chan *Message
	MessageWriter *writer
	Transport     *websocketTransport
}

// NewClient ...
func NewClient(transport *websocketTransport) *Client {

	client := &Client{
		uid:           rand.Uint32(),
		Authenticated: false,
		Subs:          make(map[string]*Channel),
		messageCh:     make(chan *Message),
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
func (c *Client) Connect(app *App) {

	node.hub.AddApp(app)
	c.App = app

	app.Clients[c.uid] = c
	app.Stats.Connections++

	c.WriteAuthPacket()
}

// Subscribe ...
func (c *Client) Subscribe(channel *Channel) {

	first := c.App.Subscribe(channel)
	channel.Clients[c.uid] = c
	c.Subs[channel.Name] = channel

	channel.Stats.Connections++

	if first {
		subKey := c.App.Key + ":" + channel.Name
		node.broker.Subscribe([]string{subKey})
	}

}

// Channels ...
func (c *Client) Channels() []string {
	var slice []string
	for t := range c.Subs {
		slice = append(slice, t)
	}

	return slice
}

// Close ...
func (c *Client) Close() {

	var toDelete []string
	for _, channel := range c.Subs {
		delete(channel.Clients, c.uid)
		channel.Stats.Connections--
		if channel.Stats.Connections == 0 {
			delete(c.App.Channels, channel.Name)
			toDelete = append(toDelete, channel.App.Key+":"+channel.Name)
		}
	}
	if len(toDelete) > 0 {
		node.broker.Unsubscribe(toDelete)
	}

	if len(c.App.Channels) == 0 {
		delete(node.hub.Apps, c.App.Key)
	}

	c.App.Stats.Connections--

	c.MessageWriter.close()
	delete(c.App.Clients, c.uid)

	c.Transport.Close(DisconnectNormal)
}

// WriteAuthPacket sends authentication confirmation message
func (c *Client) WriteAuthPacket() {
	c.MessageWriter.enqueue(NewAuthenticationSucceededMessage(c.uid).Encode())
}

func (c *Client) Handle(data []byte) {

	if len(data) == 0 {
		return
	}

	var msg *ClientMessage
	err := json.Unmarshal(data, &msg)

	if err != nil {
		log.Println(err)
		return
	}

	node.broker.Enqueue(msg)

}

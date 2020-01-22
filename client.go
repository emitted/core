package main

import (
	// "encoding/json"

	"context"
	protocol "github.com/sireax/emitted/internal/proto"
	"log"
	"math/rand"
	"sync"
)

// Client ...
type Client struct {
	mu         sync.RWMutex
	presenceMu sync.Mutex

	ctx context.Context

	authenticated bool
	closed        bool

	uid           uint32
	app           *App
	channels      map[string]*Channel
	messageWriter *writer
	transport     *websocketTransport

	pubBuffer []*Publication

	info Raw
}

// NewClient ...
func NewClient(ctx context.Context, t *websocketTransport) (*Client, error) {

	uuid := rand.Uint32()

	client := &Client{
		ctx:       ctx,
		uid:       uuid,
		channels:  make(map[string]*Channel),
		transport: t,
		pubBuffer: make([]*Publication, 0),
	}

	// Creating message writer config
	messageWriterConf := writerConfig{
		WriteFn: func(data []byte) error {
			client.transport.Write(data)
			return nil
		},
		WriteManyFn: func(data ...[]byte) error {
			for _, payload := range data {
				client.transport.Write(payload)
			}
			return nil
		},
	}

	// Setting client's message writer
	client.messageWriter = newWriter(messageWriterConf)

	return client, nil
}

func (c *Client) CloseUnauthenticated() {
	c.mu.RLock()
	c.authenticated = false
	c.closed = true
	c.mu.RUnlock()

	if !c.authenticated && !c.closed {
		c.Close()
	}
}

// Connect ...
func (c *Client) Connect(app *App) {

	node.hub.AddApp(app)
	c.app = app

	app.Clients[c.uid] = c
	app.Stats.Connections++
}

// Subscribe ...
func (c *Client) Subscribe(channel *Channel) {

	//channel, ok := c.app.Channels[channelName]
	//if !ok {
	//	c.app.Channels[channelName] = NewChannel(c.app, channelName, &ChannelOptions{
	//		AllowClientMessages: true,
	//	})
	//}

	first := c.app.Subscribe(channel)
	channel.Clients[c.uid] = c
	c.channels[channel.Name] = channel

	channel.Stats.Connections++

	if first {
		subKey := c.app.Key + ":" + channel.Name
		node.broker.Subscribe([]string{subKey})
	}

}

// Close ...
func (c *Client) Close() {
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true

	var toDelete []string
	for _, channel := range c.channels {
		delete(channel.Clients, c.uid)
		channel.Stats.Connections--
		if channel.Stats.Connections == 0 {
			delete(c.app.Channels, channel.Name)
			toDelete = append(toDelete, channel.App.Key+":"+channel.Name)
		}
	}
	if len(toDelete) > 0 {
		node.broker.Unsubscribe(toDelete)
	}

	if len(c.app.Channels) == 0 {
		delete(node.hub.Apps, c.app.Key)
	}

	c.app.Stats.Connections--

	c.messageWriter.close()
	delete(c.app.Clients, c.uid)

	c.transport.Close(DisconnectNormal)
}

func (c *Client) Unsubscribe(ch string) {
	c.mu.RLock()
	_, ok := c.channels[ch]
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		delete(c.channels, ch)
		c.mu.Unlock()
	}
}

/*
|
|
|---------------------------------------
|	Command handlers
|---------------------------------------
|
|
*/

func (c *Client) Handle(data []byte) {

	log.Println(data)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if len(data) == 0 {
		return
	}

	decoder := protocol.NewProtobufCommandDecoder(data)
	cmd, err := decoder.Decode()
	if err != nil {
		log.Fatal(err)
	}

	c.HandleCommand(cmd)

	select {
	case <-c.ctx.Done():
		return
	default:

	}

}

func (c *Client) HandleCommand(cmd *Command) *Disconnect {

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	var disconnect *Disconnect

	switch cmd.Type {
	case MethodTypeSubscribe:
		log.Println("subscribe")
		disconnect = c.handleSubscribe(cmd.Data)
	case MethodTypeUnsubscribe:
		disconnect = c.handleUnsubscribe(cmd.Data)
	case MethodTypePublish:
		disconnect = c.handlePublish(cmd.Data)
	case MethodTypePresence:
		disconnect = c.handlePresence(cmd.Data)
	}

	return disconnect
}

func (c *Client) handleSubscribe(data []byte) *Disconnect {
	var disconnect *Disconnect

	_, err := protocol.NewProtobufParamsDecoder().DecodeSubscribe(data)
	if err != nil {
		disconnect = DisconnectBadRequest
	}

	return disconnect
}

func (c *Client) handleUnsubscribe(data []byte) *Disconnect {
	var disconnect *Disconnect

	_, err := protocol.NewProtobufParamsDecoder().DecodeUnsubscribe(data)
	if err != nil {
		disconnect = DisconnectBadRequest
	}

	//c.Subscribe(msg.Channel)

	return disconnect
}

func (c *Client) handlePublish(data []byte) *Disconnect {

	var disconnect *Disconnect

	p, err := protocol.NewProtobufParamsDecoder().DecodePublish(data)
	if err != nil {
		disconnect = DisconnectBadRequest
	}

	chId := makeChId(c.app.Key, p.Channel)

	node.broker.handlePublish(chId, p)

	return disconnect
}

func (c *Client) handlePresence(data []byte) *Disconnect {

	var disconnect *Disconnect

	_, err := protocol.NewProtobufParamsDecoder().DecodePresence(data)
	if err != nil {
		disconnect = DisconnectBadRequest
	}

	return disconnect

}

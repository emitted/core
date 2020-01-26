package main

import (
	// "encoding/json"

	"context"
	protocol "github.com/sireax/emitted/internal/proto"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Client ...
type Client struct {
	mu         sync.RWMutex
	presenceMu sync.Mutex

	ctx context.Context

	authenticated bool
	closed        bool

	uid      uint32
	app      *App
	channels map[string]*Channel

	messageWriter *writer
	transport     *websocketTransport

	pubBuffer []*Publication

	expireTimer *time.Timer
	exp         int64

	staleTimer *time.Timer

	info Raw
}

// NewClient ...
func NewClient(ctx context.Context, t *websocketTransport) (*Client, error) {

	uuid := rand.Uint32()

	client := &Client{
		ctx:           ctx,
		uid:           uuid,
		channels:      make(map[string]*Channel),
		transport:     t,
		authenticated: false,
		pubBuffer:     make([]*Publication, 0),
	}

	// Creating message writer config
	messageWriterConf := writerConfig{
		WriteFn: func(data []byte) error {
			err := client.transport.Write(data)
			if err != nil {
				log.Fatalln(err)
			}
			return nil
		},
		WriteManyFn: func(data ...[]byte) error {
			for _, payload := range data {
				err := client.transport.Write(payload)
				if err != nil {
					log.Fatalln(err)
				}
			}
			return nil
		},
	}

	// Setting client's message writer
	client.messageWriter = newWriter(messageWriterConf)

	//if !client.authenticated {
	//	client.staleTimer = time.AfterFunc(time.Second * 5, client.CloseUnauthenticated)
	//}

	return client, nil
}

func (c *Client) CloseUnauthenticated() {
	c.mu.RLock()
	auth := c.authenticated
	c.mu.RUnlock()

	if !auth {
		c.Close(DisconnectExpired)
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
func (c *Client) Subscribe(ch string) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return
	}
	channel, ok := c.app.Channels[ch]
	c.mu.RUnlock()

	if ok {
		c.channels[ch] = channel
	}

}

func (c *Client) Unsubscribe(ch string) {
	c.mu.RLock()
	_, ok := c.channels[ch]
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		delete(c.channels, ch)
		c.mu.Unlock()

		leave := &Leave{
			Channel: ch,
			Info: &ClientInfo{
				Client: "lox",
			},
		}

		chId := makeChId(c.app.Key, ch)
		err := node.broker.PublishLeave(chId, leave)
		if err != nil {
			log.Fatal(err)
		}

		last, err := c.app.removeSub(ch, c)
		if last {
			node.broker.Unsubscribe([]string{chId})

			delete(c.app.Channels, ch)
		}
	}
}

func (c *Client) Expire() {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return
	}

	now := time.Now().Unix()

	exp := c.exp

	ttl := exp - now

	if ttl > 0 {
		return
	}

	c.Close(DisconnectExpired)
}

func (c *Client) Close(disconnect *Disconnect) {
	//c.presenceMu.Lock()
	//defer c.presenceMu.Unlock()
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true

	channels := make([]string, len(c.channels))

	for channel := range c.channels {
		channels = append(channels, channel)
	}

	c.mu.Unlock()

	for _, channel := range channels {
		c.Unsubscribe(channel)
	}

	if len(c.app.Channels) == 0 {
		delete(node.hub.apps, c.app.Key)
	}

	if c.expireTimer != nil {
		c.expireTimer.Stop()
	}

	c.messageWriter.close()
	delete(c.app.Clients, c.uid)

	c.transport.Close(disconnect)
}

/*
|---------------------------------------
|	Command handlers
|---------------------------------------
|
|
*/

func (c *Client) Handle(data []byte) {

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

	disconnect := c.HandleCommand(cmd)

	select {
	case <-c.ctx.Done():
		return
	default:

	}

	if disconnect != nil {
		if disconnect != DisconnectNormal {

		}
		c.Close(disconnect)
	}

}

type replyWriter struct {
	write func(*protocol.Reply) error
}

func (c *Client) HandleCommand(cmd *Command) *Disconnect {

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	var disconnect *Disconnect

	write := func(rep *protocol.Reply) error {
		rep.Id = 6346
		data, err := rep.Marshal()
		if err != nil {
			log.Fatal("error while encoding reply")
		}

		c.messageWriter.enqueue(data)

		return nil
	}

	rw := &replyWriter{write}

	switch cmd.Type {
	case MethodTypeSubscribe:
		disconnect = c.handleSubscribe(cmd.Data, rw)
	case MethodTypeUnsubscribe:
		disconnect = c.handleUnsubscribe(cmd.Data, rw)
	case MethodTypePublish:
		disconnect = c.handlePublish(cmd.Data, rw)
	case MethodTypePresence:
		disconnect = c.handlePresence(cmd.Data, rw)
	}

	return disconnect
}

func (c *Client) handleSubscribe(data []byte, rw *replyWriter) *Disconnect {
	var disconnect *Disconnect

	p, err := protocol.NewProtobufParamsDecoder().DecodeSubscribe(data)
	if err != nil {
		return DisconnectBadRequest
	}

	if p.Channel == "" {
		err := rw.write(&Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectServerError
		}

		return nil
	}

	_, ok := c.channels[p.Channel]
	if ok {
		err := rw.write(&Reply{
			Error: ErrorAlreadySubscribed,
		})
		if err != nil {
			return DisconnectServerError
		}

		return nil
	}

	first := c.app.addSub(p.Channel, c)
	chId := makeChId(c.app.Key, p.Channel)

	if first {
		node.broker.Subscribe([]string{chId})
	}

	c.Subscribe(p.Channel)

	sr := &SubscribeRequest{
		Channel: p.Channel,
	}

	err = node.broker.handleSubscribe(chId, sr)
	if err != nil {
		log.Fatal(err)
	}

	res := &SubscribeResult{
		Channel: p.Channel,
	}
	bytesRes, _ := res.Marshal()

	err = rw.write(&Reply{
		Result: bytesRes,
	})
	if err != nil {
		return DisconnectServerError
	}

	return disconnect
}

func (c *Client) handleUnsubscribe(data []byte, rw *replyWriter) *Disconnect {
	var disconnect *Disconnect

	p, err := protocol.NewProtobufParamsDecoder().DecodeUnsubscribe(data)
	if err != nil {
		return DisconnectBadRequest
	}

	if p.Channel == "" {
		err := rw.write(&Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectServerError
		}
		return nil
	}

	if _, ok := c.channels[p.Channel]; !ok {
		err := rw.write(&Reply{
			Error: ErrorChannelNotFound,
		})
		if err != nil {
			return DisconnectServerError
		}
		return nil
	}

	last, err := c.app.removeSub(p.Channel, c)

	chId := makeChId(c.app.Key, p.Channel)
	usr := &UnsubscribeRequest{
		Channel: p.Channel,
	}

	if !last {
		err = node.broker.handleUnsubscribe(chId, usr)
		if err != nil {
			log.Fatal(err)
		}
	}

	c.Unsubscribe(p.Channel)

	res := &UnsubscribeResult{}
	bytesRes, _ := res.Marshal()

	err = rw.write(&Reply{
		Result: bytesRes,
	})
	if err != nil {
		return DisconnectServerError
	}

	return disconnect
}

func (c *Client) handlePublish(data []byte, rw *replyWriter) *Disconnect {

	var disconnect *Disconnect

	p, err := protocol.NewProtobufParamsDecoder().DecodePublish(data)
	if err != nil {
		return DisconnectBadRequest
	}

	_, ok := c.channels[p.Channel]
	if !ok {
		err := rw.write(&Reply{
			Error: ErrorPermissionDenied,
		})
		if err != nil {
			return DisconnectServerError
		}
		return nil
	}

	chId := makeChId(c.app.Key, p.Channel)

	err = node.broker.handlePublish(chId, c, p)
	if err != nil {
		log.Fatal(err)
	}

	res := &PublishResult{}
	bytesRes, _ := res.Marshal()

	err = rw.write(&Reply{
		Result: bytesRes,
	})
	if err != nil {
		return DisconnectServerError
	}

	return disconnect
}

func (c *Client) handlePresence(data []byte, rw *replyWriter) *Disconnect {

	var disconnect *Disconnect

	_, err := protocol.NewProtobufParamsDecoder().DecodePresence(data)
	if err != nil {
		disconnect = DisconnectBadRequest
	}

	return disconnect

}

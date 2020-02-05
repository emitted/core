package main

import (
	// "encoding/json"

	"context"
	protocol "github.com/sireax/emitted/internal/proto"
	"github.com/sireax/emitted/internal/uuid"
	"log"
	"sync"
	"time"
)

// Client ...
type Client struct {
	mu         sync.RWMutex
	presenceMu sync.Mutex

	ctx  context.Context
	node *Node

	authenticated bool
	closed        bool

	uid string

	client  string
	version string

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

	uuid, _ := uuid.NewV4()

	log.Println(uuid.String())

	client := &Client{
		ctx:           ctx,
		uid:           uuid.String(),
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
				client.Close(DisconnectWriteError)
				//	todo: log this
			}
			return nil
		},
		WriteManyFn: func(data ...[]byte) error {
			for _, payload := range data {
				err := client.transport.Write(payload)
				if err != nil {
					client.Close(DisconnectWriteError)
				}
			}
			return nil
		},
	}

	// Setting client's message writer
	client.messageWriter = newWriter(messageWriterConf)

	if !client.authenticated {
		client.staleTimer = time.AfterFunc(time.Second*2, client.CloseUnauthenticated)
	}

	return client, nil
}

func (c *Client) clientInfo(ch string) *ClientInfo {
	channel, ok := c.channels[ch]
	if !ok {
		return nil
	}

	info, ok := channel.Info[c.uid]
	if !ok {
		return nil
	}

	return info
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

// HandleSubscribe ...
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
	}
}

func (c *Client) unsubscribeForce(ch string) {
	c.mu.RLock()
	_, ok := c.channels[ch]
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		delete(c.channels, ch)
		c.mu.Unlock()

		chId := makeChId(c.app.Key, ch)

		last, err := c.app.removeSub(ch, c)
		if last {

			node.broker.Unsubscribe(chId)

			return
		}

		leave := &Leave{
			Channel: ch,
		}

		if c.clientInfo(ch) != nil {
			leave.Info = c.clientInfo(ch)
		}

		err = node.broker.PublishLeave(chId, leave)
		if err != nil {
			log.Fatal(err)
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
		c.unsubscribeForce(channel)
	}

	if len(c.app.Channels) == 0 {
		delete(node.hub.apps, c.app.Key)
	}

	if c.expireTimer != nil {
		c.expireTimer.Stop()
	}

	err := c.messageWriter.close()
	if err != nil {
		//	todo: log this
	}
	delete(c.app.Clients, c.uid)

	err = c.transport.Close(disconnect)
	if err != nil {
		//	todo: log this
	}
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
	case MethodTypeConnect:
		disconnect = c.handleConnect(cmd.Data, rw)
	case MethodTypeSubscribe:
		disconnect = c.handleSubscribe(cmd.Data, rw)
	case MethodTypeUnsubscribe:
		disconnect = c.handleUnsubscribe(cmd.Data, rw)
	case MethodTypePublish:
		disconnect = c.handlePublish(cmd.Data, rw)
	case MethodTypePresence:
		disconnect = c.handlePresence(cmd.Data, rw)
	case MethodTypePing:
		disconnect = c.handlePing(cmd.Data, rw)
	default:
		err := rw.write(&Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	return disconnect
}

func (c *Client) handleConnect(data []byte, rw *replyWriter) *Disconnect {
	var disconnect *Disconnect

	p, err := protocol.NewProtobufParamsDecoder().DecodeConnect(data)
	if err != nil {
		return DisconnectBadRequest
	}

	if p.Version == "" {
		err := rw.write(&Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	c.mu.RLock()
	c.authenticated = true
	switch p.Client {
	case ClientTypeJs:
		c.client = "js"
	case ClientTypeSwift:
		c.client = "swift"
	default:
		return DisconnectBadRequest
	}

	c.version = p.Version

	c.staleTimer.Stop()

	c.expireTimer = time.AfterFunc(time.Hour, c.Expire)
	c.mu.RUnlock()

	expires := time.Now().Add(time.Hour).Unix()

	res := &ConnectResult{
		Uid:     c.uid,
		Expires: expires,
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

func (c *Client) handleSubscribe(data []byte, rw *replyWriter) *Disconnect {
	var disconnect *Disconnect

	p, err := protocol.NewProtobufParamsDecoder().DecodeSubscribe(data)
	if err != nil {
		err := rw.write(&Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	if p.Channel == "" {
		err := rw.write(&Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	var clientInfo ClientInfo

	switch getChannelType(p.Channel) {
	case "private":

		signature := generateSignature(c.app.Secret, c.app.Key, c.uid, p.Channel)

		ok := verifySignature(signature, p.Signature)
		if !ok {
			err := rw.write(&Reply{
				Error: ErrorInvalidSignature,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

	case "presence":

		signature := generateSignature(c.app.Secret, c.app.Key, c.uid, p.Channel)

		ok := verifySignature(signature, p.Signature)
		if !ok {
			err := rw.write(&Reply{
				Error: ErrorInvalidSignature,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

		err := clientInfo.Unmarshal(p.Data)
		if err != nil {
			err := rw.write(&Reply{
				Error: ErrorBadRequest,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

		log.Println(clientInfo)

	default:
	}

	_, ok := c.channels[p.Channel]
	if ok {
		err := rw.write(&Reply{
			Error: ErrorAlreadySubscribed,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	first := c.app.addSub(p.Channel, c, &clientInfo)
	chId := makeChId(c.app.Key, p.Channel)

	if first {
		node.broker.Subscribe(chId)
	}

	c.Subscribe(p.Channel)

	err = node.broker.HandleSubscribe(chId, p, &clientInfo)
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

	var info *ClientInfo
	info = c.clientInfo(p.Channel)

	if !last {
		err = node.broker.HandleUnsubscribe(chId, usr, info)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("This was last user")
		node.broker.Unsubscribe(chId)
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

	err = node.broker.Publish(chId, c, p)
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

	p, err := protocol.NewProtobufParamsDecoder().DecodePresence(data)
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
	}

	ch, ok := c.channels[p.Channel]
	if !ok {
		err := rw.write(&Reply{
			Error: ErrorPermissionDenied,
		})
		if err != nil {
			return DisconnectServerError
		}
	}

	presence := make(map[string]*ClientInfo, len(ch.Info))

	for uid, info := range ch.Info {
		presence[uid] = info
	}

	presenceRes := &PresenceResult{
		Presence: presence,
	}
	bytesResult, _ := presenceRes.Marshal()

	err = rw.write(&Reply{
		Result: bytesResult,
	})
	if err != nil {
		return DisconnectServerError
	}
	return disconnect

}

func (c *Client) handlePing(data []byte, rw *replyWriter) *Disconnect {

	var disconnect *Disconnect

	_, err := protocol.NewProtobufParamsDecoder().DecodePing(data)
	if err != nil {
		return DisconnectBadRequest
	}

	pingRes := &PingResult{}
	bytesRes, _ := pingRes.Marshal()

	err = rw.write(&Reply{
		Result: bytesRes,
	})
	if err != nil {
		return DisconnectServerError
	}

	return disconnect

}

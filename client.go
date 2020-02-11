package core

import (
	// "encoding/json"

	"context"
	protocol "github.com/sireax/core/internal/proto"
	"github.com/sireax/core/internal/uuid"
	"log"
	"sync"
	"time"
)

// Client ...
type Client struct {
	mu sync.RWMutex

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

	expireTimer *time.Timer
	exp         int64

	staleTimer *time.Timer
}

// NewClient ...
func NewClient(n *Node, ctx context.Context, t *websocketTransport) (*Client, error) {

	uuid, _ := uuid.NewV4()

	log.Println(uuid.String())

	client := &Client{
		ctx:           ctx,
		uid:           uuid.String(),
		node:          n,
		channels:      make(map[string]*Channel),
		transport:     t,
		authenticated: false,
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
		client.staleTimer = time.AfterFunc(time.Second*15, client.CloseUnauthenticated)
	}

	return client, nil
}

func (c *Client) clientInfo(ch string) *ClientInfo {
	c.mu.RLock()
	channel, ok := c.channels[ch]
	c.mu.RUnlock()
	if !ok {
		return nil
	}

	channel.infoMu.Lock()
	info, ok := channel.Info[c.uid]
	channel.infoMu.Unlock()

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
		c.Close(DisconnectStale)
	}
}

func (c *Client) Connect(app *App) {

	c.mu.Lock()
	c.app = app
	c.mu.Unlock()

	app.mu.Lock()
	app.Clients[c.uid] = c
	app.mu.Unlock()

	app.Stats.IncrementConns()

}

func (c *Client) Disconnect() {

	log.Println("entrance")
	c.app.mu.Lock()
	delete(c.app.Clients, c.uid)
	c.app.mu.Unlock()
	log.Println("finish")

	c.app.Stats.DecrementConns()

	if len(c.app.Channels) == 0 && c.app.Stats.getConns() == 0 {
		delete(c.node.hub.apps, c.app.Key)
	}

}

func (c *Client) Subscribe(ch string) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return
	}
	channel, ok := c.app.Channels[ch]
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		c.channels[ch] = channel
		c.mu.Unlock()
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

		chId := makeChId(c.app.Secret, ch)

		last, err := c.app.removeSub(ch, c)
		if last {

			c.node.broker.Unsubscribe(chId)

			return
		}

		leave := &Leave{
			Channel: ch,
		}

		if c.clientInfo(ch) != nil {
			leave.Info = c.clientInfo(ch)
		}

		err = c.node.broker.PublishLeave(chId, leave)
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
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true

	authenticated := c.authenticated
	c.mu.Unlock()

	log.Println(c.channels)
	channels := make([]string, 0)
	for channel := range c.channels {
		channels = append(channels, channel)
	}

	if authenticated {
		for _, channel := range channels {
			c.unsubscribeForce(channel)
		}

		c.Disconnect()
	}

	if c.expireTimer != nil {
		c.expireTimer.Stop()
	}

	err := c.messageWriter.close()
	if err != nil {
		//	todo: log this
	}

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
			Error: ErrorMethodNotFound,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	return disconnect
}

func (c *Client) handleConnect(data []byte, rw *replyWriter) *Disconnect {

	c.mu.RLock()
	if c.authenticated {
		c.mu.RUnlock()
		err := rw.write(&Reply{
			Error: ErrorAlreadyAuthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

	var disconnect *Disconnect

	p, err := protocol.NewProtobufParamsDecoder().DecodeConnect(data)
	if err != nil {
		log.Println(err)
		return DisconnectBadRequest
	}

	if p.App == "" {
		log.Println(err)
		return DisconnectBadRequest
	}

	if p.Version == "" {
		log.Println(err)
		return DisconnectBadRequest
	}

	app, err := GetApp(c.node, p.App)
	if err != nil {
		return DisconnectBadRequest
	}

	c.Connect(app)

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

	c.mu.RLock()
	if !c.authenticated {
		c.mu.RUnlock()
		err := rw.write(&Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

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

	c.mu.RLock()
	_, ok := c.channels[p.Channel]
	c.mu.RUnlock()

	if ok {
		err := rw.write(&Reply{
			Error: ErrorAlreadySubscribed,
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

	first := c.app.addSub(p.Channel, c, &clientInfo)
	chId := makeChId(c.app.Secret, p.Channel)

	c.Subscribe(p.Channel)

	if first {
		err := c.node.broker.Subscribe(chId)
		if err != nil {
			log.Fatal(err)
		}
	}

	if c.app.Options.JoinLeave {
		log.Println("publishing join")
		err = c.node.broker.HandleSubscribe(chId, p, &clientInfo)
		if err != nil {
			log.Fatal(err)
		}
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

	c.mu.RLock()
	if !c.authenticated {
		c.mu.RUnlock()
		err := rw.write(&Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

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

	chId := makeChId(c.app.Secret, p.Channel)
	usr := &UnsubscribeRequest{
		Channel: p.Channel,
	}

	var info *ClientInfo
	info = c.clientInfo(p.Channel)

	if !last {
		err = c.node.broker.HandleUnsubscribe(chId, usr, info)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		c.node.broker.Unsubscribe(chId)
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

	c.mu.RLock()
	if !c.authenticated {
		c.mu.RUnlock()
		err := rw.write(&Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

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
			return DisconnectWriteError
		}
		return nil
	}

	cInfo := c.clientInfo(p.Channel)

	chId := makeChId(c.app.Secret, p.Channel)
	err = c.node.broker.Publish(chId, cInfo, p)
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

	c.mu.RLock()
	if !c.authenticated {
		c.mu.RUnlock()
		err := rw.write(&Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

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

	switch getChannelType(p.Channel) {
	case "presence":
	default:
		err := rw.write(&Reply{
			Error: ErrorChannelNotPresence,
		})
		if err != nil {
			return DisconnectWriteError
		}
	}

	c.mu.Lock()
	ch, ok := c.channels[p.Channel]
	c.mu.Unlock()

	if !ok {
		err := rw.write(&Reply{
			Error: ErrorPermissionDenied,
		})
		if err != nil {
			return DisconnectWriteError
		}
	}

	presence := make(map[string]*ClientInfo, len(ch.Info))

	ch.infoMu.Lock()
	chUsersInfo := ch.Info
	ch.infoMu.Unlock()
	for uid, info := range chUsersInfo {
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

package core

import (
	"context"
	"github.com/emitted/core/common/proto/clientproto"
	"github.com/emitted/core/common/uuid"
	"github.com/emitted/core/common/validation"
	"io"
	"sync"
	"time"
)

const (
	channelTypePublic   string = "public"
	channelTypePrivate  string = "private"
	channelTypePresence string = "presence"
)

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

	exp int64

	authTimer  *time.Timer
	staleTimer *time.Timer
}

func NewClient(n *Node, ctx context.Context, t *websocketTransport) (*Client, error) {

	uid, _ := uuid.NewV4()

	client := &Client{
		ctx:           ctx,
		uid:           uid.String(),
		node:          n,
		channels:      make(map[string]*Channel),
		transport:     t,
		authenticated: false,
	}

	messageWriterConf := writerConfig{
		WriteFn: func(data []byte) error {
			err := client.transport.Write(data)
			if err != nil {
				client.Close(DisconnectWriteError)
				client.node.logger.log(NewLogEntry(LogLevelError, "error writing to client", map[string]interface{}{"uid": client.uid, "client": client.client, "error": err.Error()}))
			}
			return nil
		},
		WriteManyFn: func(data ...[]byte) error {
			for _, payload := range data {
				err := client.transport.Write(payload)
				if err != nil {
					client.Close(DisconnectWriteError)
					client.node.logger.log(NewLogEntry(LogLevelError, "error writing to client", map[string]interface{}{"uid": client.uid, "client": client.client, "error": err.Error()}))
				}
			}
			return nil
		},
	}

	client.messageWriter = newWriter(messageWriterConf)
	client.authTimer = time.AfterFunc(time.Second*15, client.CloseUnauthenticated)

	return client, nil
}

func (c *Client) clientInfo(ch string) *clientproto.ClientInfo {

	switch getChannelType(ch) {
	case channelTypePublic:
		fallthrough
	case channelTypePrivate:
		return nil
	case channelTypePresence:
	}

	chId := makeChId(c.app.ID, ch)

	info, err := c.node.GetPresence(chId, c.uid)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error getting client's info", map[string]interface{}{"uid": c.uid, "client": c.client, "error": err.Error()}))
	}

	return info
}

func (c *Client) CloseUnauthenticated() {
	c.mu.RLock()
	auth := c.authenticated
	c.mu.RUnlock()

	if auth == false {
		c.node.logger.log(NewLogEntry(LogLevelDebug, "closing unauthenticated client", map[string]interface{}{"uid": c.uid, "client": c.client}))
		err := c.Close(DisconnectStale)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error closing unauthenticated client", map[string]interface{}{"uid": c.uid, "client": c.client, "error": err.Error()}))
		}
	}

}

func (c *Client) closeStale() {
	c.Close(DisconnectStale)
}

func (c *Client) Connect(app *App) {

	uid := c.uid

	c.mu.Lock()
	c.app = app
	c.staleTimer = time.AfterFunc(time.Minute*2, c.closeStale)
	c.mu.Unlock()

	app.mu.Lock()
	app.clients[c.uid] = c
	app.stats.deltaConnections++
	app.mu.Unlock()

	c.node.hub.addSub(uid, c)

	numClientsGauge.Inc()
}

func (c *Client) Disconnect() {

	uid := c.uid

	c.app.mu.Lock()
	delete(c.app.clients, c.uid)
	c.app.stats.deltaConnections--
	c.app.mu.Unlock()

	c.node.hub.remSub(uid)

	numClientsGauge.Dec()

	if len(c.app.channels) == 0 && len(c.app.clients) == 0 {
		delete(c.node.hub.apps, c.app.ID)
	}

}

func (c *Client) Subscribe(ch string) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return
	}

	channel, ok := c.app.channels[ch]
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

	c.node.logger.log(NewLogEntry(LogLevelDebug, "client unsubscribed from channel", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": ch}))
}

func (c *Client) unsubscribeForce(ch string) {
	c.mu.RLock()
	_, ok := c.channels[ch]
	c.mu.RUnlock()

	uid := c.uid

	if ok {
		c.mu.Lock()
		delete(c.channels, ch)
		c.mu.Unlock()

		chId := makeChId(c.app.ID, ch)

		last, err := c.app.removeSub(ch, uid)
		if err != nil {

		}
		if last {
			err := c.node.broker.RemChannel(c.app.ID, ch)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error removing channel from redis", map[string]interface{}{"app": c.app.ID, "channel": ch, "error": err.Error()}))
			}
			err = c.node.broker.Unsubscribe(chId)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error unsubscribing from channel", map[string]interface{}{"app": c.app.ID, "channel": ch, "error": err.Error()}))
			}
		}

		switch getChannelType(ch) {

		case channelTypePrivate:
		case channelTypePublic:
		case channelTypePresence:

			clientInfo := c.clientInfo(ch)

			leave := &clientproto.Leave{
				Channel: ch,
				Data:    clientInfo,
			}

			err := c.node.RemovePresence(chId, uid)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error removing presence while force unsubscribing", map[string]interface{}{"uid": uid, "client": c.client, "app": c.app.ID, "channel": ch, "error": err.Error()}))
			}

			err = c.node.broker.PublishLeave(chId, leave)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error publishing leave while force unsubscribing", map[string]interface{}{"uid": uid, "client": c.client, "app": c.app.ID, "channel": ch, "error": err.Error()}))
			}
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

	err := c.Close(DisconnectExpired)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error closing client", map[string]interface{}{"uid": c.uid, "client": c.client, "error": err.Error()}))
	}
}

func (c *Client) Close(disconnect *Disconnect) error {

	c.mu.RLock()

	if c.closed {
		c.mu.RUnlock()
		return nil
	}

	authenticated := c.authenticated
	channels := make([]string, len(c.channels))
	for channel := range c.channels {
		channels = append(channels, channel)
	}
	c.mu.RUnlock()

	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	if authenticated {
		for _, channel := range channels {
			c.unsubscribeForce(channel)
		}

		c.Disconnect()

		if c.staleTimer != nil {
			c.staleTimer.Stop()
		}
	}

	if disconnect != nil && disconnect.Reason != "" {
		c.node.logger.log(NewLogEntry(LogLevelDebug, "closing client connection", map[string]interface{}{"uid": c.uid, "client": c.client, "reason": disconnect.Reason}))
	} else {
		c.node.logger.log(NewLogEntry(LogLevelDebug, "client closed connection", map[string]interface{}{"uid": c.uid, "client": c.client}))
	}

	err := c.messageWriter.close()
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error closing message producer", map[string]interface{}{"uid": c.uid, "client": c.client, "error": err.Error()}))
		return err
	}

	err = c.transport.Close(disconnect)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error closing transport", map[string]interface{}{"uid": c.uid, "client": c.client, "error": err.Error()}))
		return err
	}

	return nil
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
		c.node.logger.log(NewLogEntry(LogLevelInfo, "empty command received", map[string]interface{}{"uid": c.uid, "client": c.client}))
		c.Close(DisconnectBadRequest)
		return
	}

	encoder := clientproto.GetReplyEncoder()
	decoder := clientproto.GetCommandDecoder(data)

	for {
		cmd, err := decoder.Decode()
		if err != nil {
			if err == io.EOF {
				break
			}
			c.node.logger.log(NewLogEntry(LogLevelInfo, "empty command received", map[string]interface{}{"uid": c.uid, "client": c.client}))
			err := c.Close(DisconnectBadRequest)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error closing client", map[string]interface{}{"uid": c.uid, "client": c.client, "error": err.Error()}))
			}
			return
		}
		write := func(rep *clientproto.Reply) error {
			rep.Id = cmd.Id
			data, err := rep.Marshal()
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error marshaling reply", map[string]interface{}{"uid": c.uid, "client": c.client, "error": err.Error()}))
				return err
			}

			c.messageWriter.enqueue(data)

			return nil
		}
		flush := func() error {
			buf := encoder.Finish()
			if len(buf) > 0 {
				c.messageWriter.enqueue(buf)
			}
			encoder.Reset()
			return nil
		}

		select {
		case <-c.ctx.Done():
			return
		default:

		}

		disconnect := c.HandleCommand(cmd, write, flush)

		if disconnect != nil {
			if disconnect != DisconnectNormal {

			}
			err := c.Close(disconnect)
			if err != nil {

			}
		}
	}

	buf := encoder.Finish()
	if len(buf) > 0 {
		c.messageWriter.enqueue(buf)
	}

	clientproto.PutCommandDecoder(decoder)
	clientproto.PutReplyEncoder(encoder)

	return

}

type replyWriter struct {
	write func(*clientproto.Reply) error
	flush func() error
}

func (c *Client) canConnect() bool {
	c.mu.RLock()
	can := (c.app.stats.connections < c.app.MaxConnections) && !c.app.shutdown
	c.mu.RUnlock()

	return can
}

func (c *Client) canSubscribe() bool {
	return len(c.channels) < 100
}

func (c *Client) canPublish() bool {
	c.mu.RLock()
	can := c.app.Options.ClientPublications
	c.mu.RUnlock()

	if can {
		c.app.mu.RLock()
		if c.app.stats.messages < c.app.MaxMessages {
			c.app.mu.RUnlock()
			return true
		}
		c.app.mu.RUnlock()
		return false
	}

	return false
}

func (c *Client) HandleCommand(cmd *clientproto.Command, write func(reply *clientproto.Reply) error, flush func() error) *Disconnect {

	start := time.Now()

	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	var disconnect *Disconnect

	rw := &replyWriter{write, flush}

	var cmdType string

	switch cmd.Type {
	case clientproto.MethodType_CONNECT:
		cmdType = "connect"
		disconnect = c.handleConnect(cmd.Data, rw)
	case clientproto.MethodType_SUBSCRIBE:
		cmdType = "subscribe"
		disconnect = c.handleSubscribe(cmd.Data, rw)
	case clientproto.MethodType_UNSUBSCRIBE:
		cmdType = "unsubscribe"
		disconnect = c.handleUnsubscribe(cmd.Data, rw)
	case clientproto.MethodType_PUBLISH:
		cmdType = "publish"
		disconnect = c.handlePublish(cmd.Data, rw)
	case clientproto.MethodType_PRESENCE:
		cmdType = "presence"
		disconnect = c.handlePresence(cmd.Data, rw)
	case clientproto.MethodType_PING:
		cmdType = "ping"
		disconnect = c.handlePing(cmd.Data, rw)
	default:
		c.node.logger.log(NewLogEntry(LogLevelError, "client sent unknown command", map[string]interface{}{"uid": c.uid, "client": c.client}))
		err := rw.write(&clientproto.Reply{
			Error: ErrorMethodNotFound,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	duration := time.Since(start)
	commandsDurationHistogram.WithLabelValues(cmdType).Observe(duration.Seconds())

	return disconnect
}

func (c *Client) handleConnect(data []byte, rw *replyWriter) *Disconnect {

	c.mu.RLock()
	if c.authenticated {
		c.mu.RUnlock()
		err := rw.write(&clientproto.Reply{
			Error: ErrorAlreadyAuthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

	if !c.app.Active {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "client tried to connect to inactive app", map[string]interface{}{"uid": c.uid, "app": c.app.ID}))

		return DisconnectAppInactive
	}

	if !c.canConnect() {
		return DisconnectLimitExceeded
	}

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodeConnect(data)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling connection command", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "error": err.Error()}))
		return DisconnectBadRequest
	}

	if p.Version == "" {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "empty version provided in connection command", map[string]interface{}{"client": c.uid}))
		return DisconnectBadRequest
	}

	c.mu.Lock()
	c.authenticated = true

	switch p.Client {
	case clientproto.ClientType_JS:
		c.client = "js"
	case clientproto.ClientType_SWIFT:
		c.client = "swift"
	default:
		c.mu.Unlock()
		return DisconnectBadRequest
	}

	c.version = p.Version
	c.authTimer.Stop()

	c.mu.Unlock()

	c.node.logger.log(NewLogEntry(LogLevelDebug, "client connected", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "version": c.version}))

	expires := time.Now().Add(time.Hour * 6).Unix()
	res := &clientproto.ConnectResult{
		Uid:     c.uid,
		Expires: expires,
	}
	bytesRes, _ := res.Marshal()

	err = rw.write(&clientproto.Reply{
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
		err := rw.write(&clientproto.Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	if !c.canSubscribe() {
		c.mu.RUnlock()
		err := rw.write(&clientproto.Reply{
			Error: ErrorChannelLimitExceeded,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	timerSet := c.staleTimer != nil
	c.mu.RUnlock()

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodeSubscribe(data)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling subscribe command", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "error": err.Error()}))
		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	if p.Channel == "" || p.Signature == "" {
		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	if ok := validation.ChannelName(p.Channel); ok != true {
		err := rw.write(&clientproto.Reply{
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
		c.node.logger.log(NewLogEntry(LogLevelInfo, "client tried to subscribe to already subscribed channel", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel}))
		err := rw.write(&clientproto.Reply{
			Error: ErrorAlreadySubscribed,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	c.mu.RLock()
	appID := c.app.ID
	appKeys := make([]string, len(c.app.Credentials))
	for _, credentials := range c.app.Credentials {
		appKeys = append(appKeys, credentials.Key)
	}
	uid := c.uid
	c.mu.RUnlock()

	var clientInfo clientproto.ClientInfo

	switch getChannelType(p.Channel) {
	case channelTypePrivate:

		signatureVerified := false

		for i := 0; i < len(appKeys); i++ {
			if !signatureVerified {
				genSignature := generateSignature(makeChId(appID, appKeys[i]), uid, p.Channel)
				signatureVerified = verifySignature(genSignature, p.Signature)
			}
		}

		if !signatureVerified {
			c.node.logger.log(NewLogEntry(LogLevelInfo, "invalid signature provided", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel}))

			err := rw.write(&clientproto.Reply{
				Error: ErrorInvalidSignature,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

	case channelTypePresence:

		signatureVerified := false
		if p.Data == nil {
			err := rw.write(&clientproto.Reply{
				Error: ErrorBadRequest,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

		if ok := validation.ClientData(p.Data); ok != true {
			err := rw.write(&clientproto.Reply{
				Error: ErrorBadRequest,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

		for i := 0; i < len(appKeys); i++ {
			if !signatureVerified {
				signature := generatePresenceSignature(makeChId(appID, appKeys[i]), uid, p.Channel, p.Data.Id, p.Data.Data)
				signatureVerified = verifySignature(signature, p.Signature)
			}
		}

		if !signatureVerified {
			err := rw.write(&clientproto.Reply{
				Error: ErrorInvalidSignature,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

		ch, ok := c.app.channels[p.Channel]
		if ok {
			if ch.presenceExists(p.Data.Id) {
				err := rw.write(&clientproto.Reply{
					Error: ErrorPresenceAlreadyExists,
				})
				if err != nil {
					return DisconnectWriteError
				}
				return nil
			}
		}

		clientInfo = *p.Data

	default:
	}

	chId := makeChId(c.app.ID, p.Channel)

	first := c.app.addSub(p.Channel, c)

	if len(clientInfo.Data) > 0 {
		err := c.node.AddPresence(chId, c.uid, &clientInfo)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error adding presence", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
		}
	}

	c.Subscribe(p.Channel)

	if first {

		err := c.node.broker.AddChannel(appID, p.Channel)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error adding channel", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
		}

		err = c.node.broker.Subscribe(chId)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error subscribing broker to channel", map[string]interface{}{"channelID": chId, "client": c.uid, "error": err.Error()}))
		}

	}

	if getChannelType(p.Channel) == channelTypePresence {
		c.channels[p.Channel].addID(p.Data.Id)

		if c.app.Options.JoinLeave {
			err = c.node.broker.HandleSubscribe(chId, c.uid, &clientInfo, p)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error broker handling subscribe", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
			}
		}
	}

	if timerSet {
		c.mu.Lock()
		c.staleTimer.Stop()
		c.staleTimer = nil
		c.mu.Unlock()
	}

	res := &clientproto.SubscribeResult{
		Channel: p.Channel,
	}
	bytesRes, _ := res.Marshal()

	err = rw.write(&clientproto.Reply{
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
		err := rw.write(&clientproto.Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	timerSet := c.staleTimer != nil
	c.mu.RUnlock()

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodeUnsubscribe(data)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "error unmarshaling unsubscribe command", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "error": err.Error()}))
		return DisconnectBadRequest
	}

	if ok := validation.ChannelName(p.Channel); ok != true {
		err := rw.write(&clientproto.Reply{
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

	if !ok {
		err := rw.write(&clientproto.Reply{
			Error: ErrorChannelNotFound,
		})
		if err != nil {
			return DisconnectServerError
		}
		return nil
	}

	c.mu.RLock()
	uid := c.uid
	app := c.app.ID
	c.mu.RUnlock()

	last, err := c.app.removeSub(p.Channel, uid)
	if err != nil {

	}

	chId := makeChId(app, p.Channel)
	r := &clientproto.UnsubscribeRequest{
		Channel: p.Channel,
	}

	clientInfo := c.clientInfo(p.Channel)

	switch getChannelType(p.Channel) {
	case channelTypePrivate:
	case channelTypePresence:
		err := c.node.RemovePresence(p.Channel, uid)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error removing presence", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
		}

		ch := c.channels[p.Channel]
		ch.removeID(clientInfo.Id)

	case channelTypePublic:
	}

	if !last {
		if getChannelType(p.Channel) == channelTypePresence && c.app.Options.JoinLeave {
			err = c.node.broker.HandleUnsubscribe(chId, uid, clientInfo, r)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error broker handling unsubscribe", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
			}
		}
	} else {
		err := c.node.broker.RemChannel(app, p.Channel)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error removing channel from redis", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
		}
		err = c.node.broker.Unsubscribe(chId)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error unsubscribing from channel", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
		}
	}

	c.Unsubscribe(p.Channel)

	if !timerSet {
		c.mu.Lock()
		if len(c.channels) < 1 {
			c.staleTimer = time.AfterFunc(time.Minute*2, c.closeStale)
		}
		c.mu.Unlock()
	}

	res := &clientproto.UnsubscribeResult{}
	bytesRes, _ := res.Marshal()

	err = rw.write(&clientproto.Reply{
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
		err := rw.write(&clientproto.Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

	if !c.canPublish() {
		err := rw.write(&clientproto.Reply{
			Error: ErrorPermissionDenied,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodePublish(data)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "error unmarshaling publish command", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "error": err.Error()}))
		return DisconnectBadRequest
	}

	if ok := validation.ChannelName(p.Channel); ok != true {
		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	if ok := validation.TopicName(p.Topic); ok != true {
		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	_, ok := c.channels[p.Channel]
	if !ok {
		err := rw.write(&clientproto.Reply{
			Error: ErrorPermissionDenied,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	clientInfo := c.clientInfo(p.Channel)

	c.mu.RLock()
	app := c.app.ID
	c.mu.RUnlock()

	chId := makeChId(app, p.Channel)

	err = c.node.broker.Publish(chId, clientInfo, p)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error broker handling publication", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
	}

	res := &clientproto.PublishResult{}
	bytesRes, _ := res.Marshal()

	err = rw.write(&clientproto.Reply{
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
		err := rw.write(&clientproto.Reply{
			Error: ErrorUnauthorized,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}
	c.mu.RUnlock()

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodePresence(data)
	if err != nil {
		return DisconnectBadRequest
	}

	channel := p.Channel

	if channel == "" {

		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectServerError
		}
	}

	if ok := validation.ChannelName(channel); ok != true {
		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	switch getChannelType(p.Channel) {
	case channelTypePresence:
	default:
		err := rw.write(&clientproto.Reply{
			Error: ErrorChannelNotPresence,
		})
		if err != nil {
			return DisconnectWriteError
		}
	}

	c.mu.Lock()
	_, ok := c.channels[channel]
	c.mu.Unlock()

	if !ok {
		err := rw.write(&clientproto.Reply{
			Error: ErrorPermissionDenied,
		})
		if err != nil {
			return DisconnectWriteError
		}
	}

	c.mu.RLock()
	app := c.app.ID
	c.mu.RUnlock()

	chId := makeChId(app, channel)

	presence, err := c.node.Presence(chId)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error getting presence", map[string]interface{}{"uid": c.uid, "client": c.client, "app": c.app.ID, "channel": p.Channel, "error": err.Error()}))
	}

	presenceRes := &clientproto.PresenceResult{
		Presence: presence,
	}
	bytesResult, _ := presenceRes.Marshal()

	err = rw.write(&clientproto.Reply{
		Result: bytesResult,
	})
	if err != nil {
		return DisconnectServerError
	}
	return disconnect

}

func (c *Client) handlePing(data []byte, rw *replyWriter) *Disconnect {

	var disconnect *Disconnect

	_, err := clientproto.NewProtobufParamsDecoder().DecodePing(data)
	if err != nil {
		return DisconnectBadRequest
	}

	pingRes := &clientproto.PingResult{}
	bytesRes, _ := pingRes.Marshal()

	err = rw.write(&clientproto.Reply{
		Result: bytesRes,
	})
	if err != nil {
		return DisconnectServerError
	}

	return disconnect

}

package core

import (
	"context"
	"github.com/sireax/core/common/proto/clientproto"
	"github.com/sireax/core/common/proto/webhooks"
	"github.com/sireax/core/common/uuid"
	"io"
	"sync"
	"time"
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

	expireTimer *time.Timer
	exp         int64

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

	// Creating message producer config
	messageWriterConf := writerConfig{
		WriteFn: func(data []byte) error {
			err := client.transport.Write(data)
			if err != nil {
				client.Close(DisconnectWriteError)
				client.node.logger.log(NewLogEntry(LogLevelError, "error writing to client", map[string]interface{}{"client": client.uid, "error": err.Error()}))
			}
			return nil
		},
		WriteManyFn: func(data ...[]byte) error {
			for _, payload := range data {
				err := client.transport.Write(payload)
				if err != nil {
					client.Close(DisconnectWriteError)
					client.node.logger.log(NewLogEntry(LogLevelError, "error writing to client", map[string]interface{}{"client": client.uid, "error": err.Error()}))
				}
			}
			return nil
		},
	}

	client.messageWriter = newWriter(messageWriterConf)
	client.staleTimer = time.AfterFunc(time.Second*15, client.CloseUnauthenticated)

	return client, nil
}

func (c *Client) clientInfo(ch string) *clientproto.ClientInfo {

	switch getChannelType(ch) {
	case "public":
		fallthrough
	case "private":
		return nil
	case "presence":
	}

	chId := makeChId(c.app.ID, ch)

	info, err := c.node.GetPresence(chId, c.uid)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error getting client's info", map[string]interface{}{"error": err.Error()}))
	}

	return info
}

func (c *Client) CloseUnauthenticated() {
	c.mu.RLock()
	auth := c.authenticated
	c.mu.RUnlock()

	if auth == false {
		c.node.logger.log(NewLogEntry(LogLevelDebug, "closing unauthenticated client", map[string]interface{}{"client": c.uid}))
		err := c.Close(DisconnectStale)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error closing client", map[string]interface{}{"client": c.uid, "error": err.Error()}))
		}
	}

}

func (c *Client) Connect(app *App) {

	c.mu.Lock()
	c.app = app
	c.mu.Unlock()

	app.mu.Lock()
	app.Clients[c.uid] = c
	app.Stats.Connections++
	app.mu.Unlock()

	c.node.hub.addSub(c)

	numClientsGauge.Inc()
}

func (c *Client) Disconnect() {

	c.app.mu.Lock()
	delete(c.app.Clients, c.uid)
	c.app.Stats.Connections--
	c.app.mu.Unlock()

	c.node.hub.remSub(c)

	numClientsGauge.Dec()

	if len(c.app.Channels) == 0 && c.app.Stats.Connections == 0 {
		delete(c.node.hub.apps, c.app.ID)
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

	c.node.logger.log(NewLogEntry(LogLevelDebug, "client has subscribed to channel", map[string]interface{}{"client": c.uid, "app": c.app.ID, "channel": ch}))

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

	c.node.logger.log(NewLogEntry(LogLevelDebug, "client has unsubscribed from channel", map[string]interface{}{"client": c.uid, "app": c.app.ID, "channel": ch}))
}

func (c *Client) unsubscribeForce(ch string) {
	c.mu.RLock()
	_, ok := c.channels[ch]
	uid := c.uid
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		delete(c.channels, ch)
		c.mu.Unlock()

		chId := makeChId(c.app.ID, ch)

		last, err := c.app.removeSub(ch, uid)
		if last {
			err := c.node.broker.RemChannel(c.app.ID, ch)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error removing channel from redis", map[string]interface{}{"channel": ch, "error": err.Error()}))
			}
			err = c.node.broker.Unsubscribe(chId)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error unsubscribing from channel", map[string]interface{}{"channel": ch, "error": err.Error()}))
			}
		}

		var clientInfo *clientproto.ClientInfo

		switch getChannelType(ch) {
		case "private":
		case "presence":
			clientInfo = c.clientInfo(ch)
		case "public":
		}
		leave := &clientproto.Leave{
			Channel: ch,
			Data:    clientInfo,
		}

		c.mu.RLock()
		uid := c.uid
		c.mu.RUnlock()

		switch getChannelType(ch) {
		case "private":
		case "presence":
			err := c.node.RemovePresence(chId, uid)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error removing presence", map[string]interface{}{"uid": uid, "error": err.Error()}))
			}
		case "public":
		}

		err = c.node.broker.PublishLeave(chId, leave)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error publishing leave", map[string]interface{}{"uid": c.uid, "error": err.Error()}))
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

func (c *Client) Close(disconnect *Disconnect) error {
	c.mu.RLock()

	if c.closed {
		c.mu.RUnlock()
		return nil
	}
	c.closed = true

	authenticated := c.authenticated
	c.mu.RUnlock()

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

	//if c.expireTimer != nil {
	//	c.expireTimer.Stop()
	//}

	if disconnect != nil && disconnect.Reason != "" {
		c.node.logger.log(NewLogEntry(LogLevelDebug, "closing client connection", map[string]interface{}{"client": c.uid, "reason": disconnect.Reason}))
	} else {
		c.node.logger.log(NewLogEntry(LogLevelDebug, "client closed connection", map[string]interface{}{"client": c.uid, "app": c.app.ID}))
	}

	err := c.messageWriter.close()
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error closing message producer", map[string]interface{}{"client": c.uid, "error": err.Error()}))
		return err
	}

	err = c.transport.Close(disconnect)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error closing transport", map[string]interface{}{"client": c.uid, "error": err.Error()}))
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
		c.node.logger.log(NewLogEntry(LogLevelInfo, "empty command received", map[string]interface{}{"client": c.uid}))
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
			c.node.logger.log(NewLogEntry(LogLevelInfo, "empty command received", map[string]interface{}{"client": c.uid}))
			err := c.Close(DisconnectBadRequest)
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error closing client", map[string]interface{}{"client": c.uid, "error": err.Error()}))
			}
			return
		}
		write := func(rep *clientproto.Reply) error {
			rep.Id = cmd.Id
			data, err := rep.Marshal()
			if err != nil {
				c.node.logger.log(NewLogEntry(LogLevelError, "error marshaling reply", map[string]interface{}{"client": c.uid, "error": err.Error()}))
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
		disconnect := c.HandleCommand(cmd, write, flush)

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
	can := (c.app.Stats.Connections < c.app.MaxConnections) && !c.app.shutdown
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
		if c.app.Stats.Messages < c.app.MaxMessages {
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

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

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

	if !c.app.checkDue() {
		c.node.logger.log(NewLogEntry(LogLevelDebug, "client tried to connect to unpaid app"))

		return DisconnectSubscriptionEnded
	}

	if !c.canConnect() {
		return DisconnectLimitExceeded
	}

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodeConnect(data)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "error unmarshaling connection command", map[string]interface{}{"client": c.uid, "error": err.Error()}))
		return DisconnectBadRequest
	}

	if p.Version == "" {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "empty version provided in connection command", map[string]interface{}{"client": c.uid}))
		return DisconnectBadRequest
	}

	c.mu.RLock()
	c.authenticated = true

	switch p.Client {
	case clientproto.ClientType_JS:
		c.client = "js"
	case clientproto.ClientType_SWIFT:
		c.client = "swift"
	default:
		c.mu.RUnlock()
		return DisconnectBadRequest
	}

	c.version = p.Version
	c.staleTimer.Stop()

	c.mu.RUnlock()

	c.node.logger.log(NewLogEntry(LogLevelDebug, "a client connected", map[string]interface{}{"uid": c.uid, "app": c.app.ID, "client": c.client, "version": c.version}))

	expires := time.Now().Add(time.Hour).Unix()
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
	c.mu.RUnlock()

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodeSubscribe(data)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "error unmarshaling subscribe command", map[string]interface{}{"client": c.uid, "error": err.Error()}))
		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectWriteError
		}
		return nil
	}

	channel := p.Channel

	if channel == "" {
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
		err := rw.write(&clientproto.Reply{
			Error: ErrorAlreadySubscribed,
		})
		if err != nil {
			return DisconnectWriteError
		}

		return nil
	}

	c.mu.RLock()
	app := c.app.ID
	uid := c.uid
	c.mu.RUnlock()

	var clientInfo clientproto.ClientInfo

	switch getChannelType(channel) {
	case "private":

		signature := generateSignature(app, uid, channel)

		ok := verifySignature(signature, p.Signature)
		if !ok {
			err := rw.write(&clientproto.Reply{
				Error: ErrorInvalidSignature,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

	case "presence":

		signature := generatePresenceSignature(app, uid, channel, p.Data.Id, p.Data.Data)

		ok := verifySignature(signature, p.Signature)
		if !ok {
			err := rw.write(&clientproto.Reply{
				Error: ErrorInvalidSignature,
			})
			if err != nil {
				return DisconnectWriteError
			}
			return nil
		}

		clientInfo = *p.Data

	default:
	}

	chId := makeChId(c.app.ID, p.Channel)

	first := c.app.addSub(p.Channel, c)

	if len(clientInfo.Data) > 0 {
		err := c.node.AddPresence(chId, c.uid, &clientInfo)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error adding presence", map[string]interface{}{"error": err.Error()}))
		}
	}

	c.Subscribe(p.Channel)

	if first {

		err := c.node.broker.AddChannel(app, p.Channel)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error adding channel", map[string]interface{}{"error": err.Error()}))
		}

		err = c.node.broker.Subscribe(chId)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error subscribing broker to channel", map[string]interface{}{"channelID": chId, "client": c.uid, "error": err.Error()}))
		}

	}

	if c.app.Options.JoinLeave {
		err = c.node.broker.HandleSubscribe(chId, c.uid, &clientInfo, p)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error broker handling subscribe", map[string]interface{}{"channelId": chId, "error": err.Error()}))
		}
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

	////////////
	// Webhooks
	////////////

	//if getChannelType(p.Channel) == "presence" {
	//
	//	go func() {
	//
	//		var info webhooks.ClientInfo
	//
	//		if p.Data != nil {
	//			info = webhooks.ClientInfo{
	//				Id:   p.Data.Id,
	//				Data: p.Data.Data,
	//			}
	//		}
	//
	//		joinWh := webhooks.PresenceAdded{
	//			Channel: p.Channel,
	//			Uid:     uid,
	//			Info:    &info,
	//		}
	//
	//		joinWhData, _ := joinWh.Marshal()
	//
	//		for _, webhook := range c.app.Options.Webhooks {
	//
	//			if !webhook.Presence {
	//				continue
	//			}
	//
	//			wh := webhooks.Webhook{
	//				Id:        0,
	//				Signature: "",
	//				Event:     webhooks.Event_PRESENCE_ADDED,
	//				AppId:     c.app.ID,
	//				Url:       webhook.Url,
	//				Data:      joinWhData,
	//			}
	//
	//			whData, _ := wh.Marshal()
	//
	//			err := c.node.webhook.Enqueue(webhookRequest{
	//				data: whData,
	//			})
	//			if err != nil {
	//				c.node.logger.log(NewLogEntry(LogLevelError, "error enqueuing webhook", map[string]interface{}{"error": err.Error()}))
	//			}
	//
	//		}
	//
	//	}()
	//
	//}

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
	c.mu.RUnlock()

	var disconnect *Disconnect

	p, err := clientproto.NewProtobufParamsDecoder().DecodeUnsubscribe(data)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelInfo, "error unmarshaling unsubscribe command", map[string]interface{}{"client": c.uid, "error": err.Error()}))
		return DisconnectBadRequest
	}

	if p.Channel == "" {
		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectServerError
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

	chId := makeChId(app, p.Channel)
	r := &clientproto.UnsubscribeRequest{
		Channel: p.Channel,
	}

	switch getChannelType(p.Channel) {
	case "private":
	case "presence":
		err := c.node.RemovePresence(p.Channel, uid)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error removing presence", map[string]interface{}{"error": err.Error()}))
		}

	case "public":
	}

	clientInfo := c.clientInfo(p.Channel)

	if !last {
		err = c.node.broker.HandleUnsubscribe(chId, uid, clientInfo, r)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error broker handling unsubscribe", map[string]interface{}{"channelId": chId, "error": err.Error()}))
		}
	} else {
		err := c.node.broker.RemChannel(app, p.Channel)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error deleting channel from redis", map[string]interface{}{"error": err.Error()}))
		}
		err = c.node.broker.Unsubscribe(chId)
		if err != nil {
			c.node.logger.log(NewLogEntry(LogLevelError, "error unsubscribing from channel", map[string]interface{}{"error": err.Error()}))
		}
	}

	c.Unsubscribe(p.Channel)

	res := &clientproto.UnsubscribeResult{}
	bytesRes, _ := res.Marshal()

	err = rw.write(&clientproto.Reply{
		Result: bytesRes,
	})
	if err != nil {
		return DisconnectServerError
	}

	////////////
	// Webhooks
	////////////

	go func() {

		info := webhooks.ClientInfo{
			Id:   clientInfo.Id,
			Data: clientInfo.Data,
		}
		leaveWh := webhooks.PresenceRemoved{
			Channel: p.Channel,
			Uid:     uid,
			Info:    &info,
		}

		leaveWhData, _ := leaveWh.Marshal()
		for _, webhook := range c.app.Options.Webhooks {

			if !webhook.Presence {
				continue
			}

			wh := webhooks.Webhook{
				Id:        0,
				Signature: "",
				Event:     webhooks.Event_PRESENCE_REMOVED,
				AppId:     c.app.ID,
				Url:       webhook.Url,
				Data:      leaveWhData,
			}

			whData, _ := wh.Marshal()

			whR := webhookRequest{
				data: whData,
			}

			err := c.node.webhook.Enqueue(whR)
			if err != nil {

			}
		}

	}()

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
		c.node.logger.log(NewLogEntry(LogLevelInfo, "error unmarshaling publish command", map[string]interface{}{"client": c.uid, "error": err.Error()}))
		return DisconnectBadRequest
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
		c.node.logger.log(NewLogEntry(LogLevelError, "error broker handling publication", map[string]interface{}{"channelID": chId, "client": c.uid, "error": err.Error()}))
	}

	res := &clientproto.PublishResult{}
	bytesRes, _ := res.Marshal()

	err = rw.write(&clientproto.Reply{
		Result: bytesRes,
	})
	if err != nil {
		return DisconnectServerError
	}

	////////////
	// Webhooks
	////////////

	//go func() {
	//
	//	var info webhooks.ClientInfo
	//
	//	if clientInfo != nil {
	//		info = webhooks.ClientInfo{
	//			Id:   clientInfo.Id,
	//			Data: clientInfo.Data,
	//		}
	//	}
	//
	//	pubWh := webhooks.Publication{
	//		Channel: p.Channel,
	//		Uid:     c.uid,
	//		Data:    p.Data,
	//		Info:    &info,
	//	}
	//
	//	pubWhBytes, _ := pubWh.Marshal()
	//
	//	for _, webhook := range c.app.Options.Webhooks {
	//
	//		if !webhook.Publication {
	//			continue
	//		}
	//
	//		wh := webhooks.Webhook{
	//			Id:        0,
	//			Timestamp: time.Now().Unix(),
	//			Signature: "",
	//			Event:     webhooks.Event_PUBLICATION,
	//			AppId:     c.app.ID,
	//			Url:       webhook.Url,
	//			Data:      pubWhBytes,
	//		}
	//
	//		c.node.logger.log(NewLogEntry(LogLevelDebug, "sending webhook", map[string]interface{}{"wh": wh}))
	//
	//		whBytes, _ := wh.Marshal()
	//
	//		r := webhookRequest{
	//			data: whBytes,
	//		}
	//
	//		err := c.node.webhook.Enqueue(r)
	//		if err != nil {
	//			c.node.logger.log(NewLogEntry(LogLevelError, "error enqueuing webhook", map[string]interface{}{"error": err.Error()}))
	//		}
	//
	//	}
	//}()

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

	if p.Channel == "" {

		err := rw.write(&clientproto.Reply{
			Error: ErrorBadRequest,
		})
		if err != nil {
			return DisconnectServerError
		}
	}

	switch getChannelType(p.Channel) {
	case "presence":
	default:
		err := rw.write(&clientproto.Reply{
			Error: ErrorChannelNotPresence,
		})
		if err != nil {
			return DisconnectWriteError
		}
	}

	c.mu.Lock()
	_, ok := c.channels[p.Channel]
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

	chId := makeChId(app, p.Channel)

	presence, err := c.node.Presence(chId)
	if err != nil {
		c.node.logger.log(NewLogEntry(LogLevelError, "error getting presence", map[string]interface{}{"error": err.Error()}))
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

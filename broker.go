package core

import (
	"errors"
	"fmt"
	"github.com/emitted/core/common/proto/clientproto"
	"github.com/emitted/core/common/timers"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	defaultReadTimeout    = time.Second
	defaultWriteTimeout   = time.Second
	defaultConnectTimeout = time.Second
	defaultPoolSize       = 256
)

var (
	addPresenceSource = `redis.call("hset", KEYS[1], ARGV[1], ARGV[2])`

	remPresenceSource = `redis.call("hdel", KEYS[1], ARGV[1])`

	presenceSource = `return redis.call("hgetall", KEYS[1])`

	getPresenceSource = `return redis.call("hget", KEYS[1], ARGV[1])`

	updateStatsSource = `
redis.call("hincrby", KEYS[1], ARGV[1], ARGV[2])
redis.call("hincrby", KEYS[1], ARGV[3], ARGV[4])
`

	retrieveStatsSource = `return redis.call("hgetall", KEYS[1])`

	countChannelsSource = `return redis.call("scard", KEYS[1])`

	channelsSource = `return redis.call("smembers", KEYS[1])`

	addChannelSource = `return redis.call("sadd", KEYS[1], ARGV[1])`

	remChannelSource = `return redis.call("srem", KEYS[1], ARGV[1])`
)

type Broker struct {
	node   *Node
	shards []*shard
	config *BrokerConfig
}

type BrokerConfig struct {
	Shards []BrokerShardConfig
}

type shard struct {
	node   *Node
	broker *Broker
	config BrokerShardConfig
	pool   *redis.Pool

	subCh        chan subRequest
	subMessages  chan redis.Message
	pubMessages  chan pubRequest
	dataMessages chan dataRequest

	getPresenceScript   *redis.Script
	presenceScript      *redis.Script
	addPresenceScript   *redis.Script
	remPresenceScript   *redis.Script
	updateStatsScript   *redis.Script
	retrieveStatsScript *redis.Script
	countChannelsScript *redis.Script
	channelsScript      *redis.Script
	addChannelScript    *redis.Script
	remChannelScript    *redis.Script
}

type BrokerShardConfig struct {
	Host             string
	Port             int
	Password         string
	DB               int
	UseTLS           bool
	TLSSkipVerify    bool
	MasterName       string
	IdleTimeout      time.Duration
	PubSubNumWorkers int
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	ConnectTimeout   time.Duration
}

// NewBroker function initializes BrokerMessage Broker
func NewBroker(n *Node, config *BrokerConfig) (*Broker, error) {

	var shards []*shard

	if len(config.Shards) == 0 {
		return nil, errors.New("no Redis shards provided in configuration")
	}

	for _, conf := range config.Shards {
		shard, err := NewShard(n, conf)
		if err != nil {
			return nil, err
		}
		shards = append(shards, shard)
	}

	return &Broker{
		node:   n,
		shards: shards,
		config: config,
	}, nil
}

func (b *Broker) Run() error {
	for _, shard := range b.shards {
		shard.broker = b
		err := shard.Run()
		if err != nil {
			return err
		}
	}
	return nil
}

//////////////////////
////// REDIRECTS /////

func (b *Broker) PublishNode(data []byte) error {
	var err error
	for _, shard := range b.shards {
		err = shard.PublishNode(data)
		if err != nil {
			continue
		}
		return nil
	}
	return fmt.Errorf("publish node error, all shards failed: last error: %v", err)
}

func (b *Broker) Subscribe(chId string) error {
	return b.getShard(chId).Subscribe([]string{chId})
}

func (b *Broker) Unsubscribe(chId string) error {
	return b.getShard(chId).Unsubscribe([]string{chId})
}

func (b *Broker) Publish(chId string, clientInfo *clientproto.ClientInfo, p *clientproto.PublishRequest) error {
	return b.getShard(chId).handlePublish(chId, clientInfo, p)
}

func (b *Broker) HandleSubscribe(chId string, uid string, clientInfo *clientproto.ClientInfo, r *clientproto.SubscribeRequest) error {
	return b.getShard(chId).handleSubscribe(chId, uid, clientInfo, r)
}

func (b *Broker) HandleUnsubscribe(chId string, uid string, clientInfo *clientproto.ClientInfo, r *clientproto.UnsubscribeRequest) error {
	return b.getShard(chId).handleUnsubscribe(chId, uid, clientInfo, r)
}

func (b *Broker) PublishJoin(chId string, join *clientproto.Join) error {
	return b.getShard(chId).PublishJoin(chId, join)
}

func (b *Broker) PublishLeave(chId string, leave *clientproto.Leave) error {
	return b.getShard(chId).PublishLeave(chId, leave)
}

func (b *Broker) AddPresence(ch string, uid string, clientInfo *clientproto.ClientInfo) error {
	return b.getShard(ch).addPresence(ch, uid, clientInfo)
}

func (b *Broker) RemovePresence(ch, uid string) error {
	return b.getShard(ch).removePresence(ch, uid)
}

func (b *Broker) Presence(ch string) (map[string]*clientproto.ClientInfo, error) {
	return b.getShard(ch).presence(ch)
}

func (b *Broker) GetPresence(ch, uid string) (*clientproto.ClientInfo, error) {
	return b.getShard(ch).getPresence(ch, uid)
}

func (b *Broker) UpdateStats(app string, conns, msgs int) error {
	return b.getShard(app).updateStats(app, conns, msgs)
}

func (b *Broker) RetrieveStats(app string) (int, int, error) {
	return b.getShard(app).retrieveStats(app)
}

func (b *Broker) Channels(app string) ([]string, error) {
	return b.getShard(app).Channels(app)
}

func (b *Broker) AddChannel(app, channel string) error {
	return b.getShard(app).addChannel(app, channel)
}

func (b *Broker) RemChannel(app, channel string) error {
	return b.getShard(app).remChannel(app, channel)
}

func (b *Broker) CountChannels(app string) (int, error) {
	return b.getShard(app).countChannels(app)
}

func newPool(n *Node, conf BrokerShardConfig) *redis.Pool {
	host := conf.Host
	port := conf.Port
	password := conf.Password
	db := conf.DB

	serverAddr := net.JoinHostPort(host, strconv.Itoa(port))

	poolSize := defaultPoolSize

	maxIdle := 64

	return &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   poolSize,
		Wait:        true,
		IdleTimeout: conf.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			var err error

			var readTimeout = defaultReadTimeout
			if conf.ReadTimeout != 0 {
				readTimeout = conf.ReadTimeout
			}
			var writeTimeout = defaultWriteTimeout
			if conf.WriteTimeout != 0 {
				writeTimeout = conf.WriteTimeout
			}
			var connectTimeout = defaultConnectTimeout
			if conf.ConnectTimeout != 0 {
				connectTimeout = conf.ConnectTimeout
			}

			opts := []redis.DialOption{
				redis.DialConnectTimeout(connectTimeout),
				redis.DialReadTimeout(readTimeout),
				redis.DialWriteTimeout(writeTimeout),
			}
			c, err := redis.Dial("tcp", serverAddr, opts...)
			if err != nil {
				return nil, err
			}

			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}

			if db != 0 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (s *shard) presenceHashKey(chID string) string {
	return "client.presence.data." + chID
}

func (s *shard) statsHashKey(app string) string {
	return "app.stats." + app
}

func (s *shard) channelsHashKey(app string) string {
	return "app.channels." + app
}

func (s *shard) addPresence(ch, uid string, clientInfo *clientproto.ClientInfo) error {

	infoBytes, err := clientInfo.Marshal()
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error marshaling client info", map[string]interface{}{"error": err.Error()}))
	}
	hashKey := s.presenceHashKey(ch)

	dr := newDataRequest(dataOpAddPresence, []interface{}{hashKey, uid, infoBytes})
	resp := s.getDataResponse(dr)

	return resp.err
}

func (s *shard) removePresence(ch, uid string) error {
	hashKey := s.presenceHashKey(ch)

	dr := newDataRequest(dataOpRemovePresence, []interface{}{hashKey, uid})
	resp := s.getDataResponse(dr)

	return resp.err
}

func (s *shard) presence(ch string) (map[string]*clientproto.ClientInfo, error) {
	hashKey := s.presenceHashKey(ch)

	dr := newDataRequest(dataOpPresence, []interface{}{hashKey})
	resp := s.getDataResponse(dr)

	return s.mapStringClientInfoId(resp.reply, nil)
}

func (s *shard) getPresence(ch, uid string) (*clientproto.ClientInfo, error) {
	hashKey := s.presenceHashKey(ch)

	dr := newDataRequest(dataOpGetPresence, []interface{}{hashKey, uid})
	resp := s.getDataResponse(dr)

	var clientInfo clientproto.ClientInfo
	err := clientInfo.Unmarshal(resp.reply.([]byte))
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling client info", map[string]interface{}{"error": err.Error()}))
	}

	return &clientInfo, resp.err
}

func (s *shard) updateStats(app string, conns, msgs int) error {
	hashKey := s.statsHashKey(app)

	dr := newDataRequest(dataOpUpdateStats, []interface{}{hashKey, "connections", conns, "messages", msgs})
	resp := s.getDataResponse(dr)

	return resp.err
}

func (s *shard) retrieveStats(app string) (int, int, error) {
	hashKey := s.statsHashKey(app)

	dr := newDataRequest(dataOpRetrieveStats, []interface{}{hashKey})
	resp := s.getDataResponse(dr)

	return s.mapStringStats(resp.reply, nil)
}

func (s *shard) addChannel(app, channel string) error {
	hashKey := s.channelsHashKey(app)

	dr := newDataRequest(dataOpAddChannel, []interface{}{hashKey, channel})
	resp := s.getDataResponse(dr)

	return resp.err
}

func (s *shard) remChannel(app, channel string) error {
	hashKey := s.channelsHashKey(app)

	dr := newDataRequest(dataOpRemChannel, []interface{}{hashKey, channel})
	resp := s.getDataResponse(dr)

	return resp.err
}

func (s *shard) Channels(app string) ([]string, error) {

	channelsHashKey := s.channelsHashKey(app)

	dr := newDataRequest(dataOpChannels, []interface{}{channelsHashKey})
	resp := s.getDataResponse(dr)

	if resp.err != nil {
		return nil, resp.err
	}

	values, err := redis.Values(resp.reply, nil)
	if err != nil {
		return nil, err
	}

	channels := make([]string, 0, len(values))
	for i := 0; i < len(values); i += 2 {
		value, okValue := values[i].([]byte)
		if !okValue {
			return nil, errors.New("error getting ChannelID value")
		}

		channels = append(channels, string(value))
	}

	return channels, nil
}

func (s *shard) countChannels(app string) (int, error) {

	channelsHashKey := s.channelsHashKey(app)

	dr := newDataRequest(dataOpCountChannels, []interface{}{channelsHashKey})
	resp := s.getDataResponse(dr)

	if resp.err != nil {
		return 0, resp.err
	}

	num := int(resp.reply.(int64))

	return num, nil
}

func (s *shard) mapStringClientInfoUid(reply interface{}, err error) (map[string]*clientproto.ClientInfo, error) {
	values, err := redis.Values(reply, err)

	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("mapStringClientInfoUid expects even number of values result")
	}

	m := make(map[string]*clientproto.ClientInfo, len(values))

	for i := 0; i < len(values); i += 2 {
		key, okKey := values[i].([]byte)
		value, okValue := values[i+1].([]byte)
		if !okKey || !okValue {
			return nil, errors.New("scanMap key not a bulk string value")
		}

		var clientInfo clientproto.ClientInfo
		err := clientInfo.Unmarshal(value)
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling client info", map[string]interface{}{"error": err.Error()}))
		}

		m[string(key)] = &clientInfo
	}

	return m, nil
}

func (s *shard) mapStringClientInfoId(reply interface{}, err error) (map[string]*clientproto.ClientInfo, error) {
	values, err := redis.Values(reply, err)

	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("mapStringClientInfoUid expects even number of values result")
	}

	m := make(map[string]*clientproto.ClientInfo, len(values))

	for i := 0; i < len(values); i += 2 {
		value, okValue := values[i+1].([]byte)
		if !okValue {
			return nil, errors.New("scanMap key not a bulk string value")
		}

		var clientInfo clientproto.ClientInfo
		err := clientInfo.Unmarshal(value)
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling client info", map[string]interface{}{"error": err.Error()}))
		}

		m[clientInfo.Id] = &clientInfo
	}

	return m, nil
}

func (s *shard) mapStringStats(reply interface{}, err error) (int, int, error) {
	values, err := redis.Values(reply, err)

	if err != nil {
		return 0, 0, err
	}
	if len(values)%2 != 0 {
		return 0, 0, errors.New("mapStringStats expects even number of values result")
	}

	stats := make(map[string]int, len(values))

	for i := 0; i < len(values); i += 2 {
		key, okKey := values[i].([]byte)
		value, okValue := values[i+1].([]byte)

		if !okKey || !okValue {
			return 0, 0, errors.New("scanMap key not a bulk string value")
		}

		integer, err := strconv.Atoi(string(value))
		if err != nil {

		}

		stats[string(key)] = integer
	}

	return stats["connections"], stats["messages"], nil
}

func (s *shard) getDataResponse(r dataRequest) *dataResponse {
	select {
	case s.dataMessages <- r:
	default:
		timer := timers.SetTimer(time.Second * 5)
		defer timers.ReleaseTimer(timer)
		select {
		case s.dataMessages <- r:
		case <-timer.C:
			return &dataResponse{r.result(), errors.New("redis timeout")}
		}
	}
	return r.result()
}

func NewShard(n *Node, conf BrokerShardConfig) (*shard, error) {
	shard := &shard{
		node:   n,
		config: conf,
		pool:   newPool(n, conf),

		pubMessages:  make(chan pubRequest),
		subCh:        make(chan subRequest),
		subMessages:  make(chan redis.Message),
		dataMessages: make(chan dataRequest),

		presenceScript:      redis.NewScript(1, presenceSource),
		getPresenceScript:   redis.NewScript(1, getPresenceSource),
		addPresenceScript:   redis.NewScript(1, addPresenceSource),
		remPresenceScript:   redis.NewScript(1, remPresenceSource),
		updateStatsScript:   redis.NewScript(1, updateStatsSource),
		retrieveStatsScript: redis.NewScript(1, retrieveStatsSource),
		channelsScript:      redis.NewScript(1, channelsSource),
		addChannelScript:    redis.NewScript(1, addChannelSource),
		remChannelScript:    redis.NewScript(1, remChannelSource),
		countChannelsScript: redis.NewScript(1, countChannelsSource),
	}
	return shard, nil
}

func (b *Broker) getShard(channel string) *shard {
	return b.shards[consistentIndex(channel, len(b.shards))]
}

func (s *shard) Run() error {

	go runForever(func() {
		s.runPublishPipeline()
	})

	go runForever(func() {
		s.RunDataPipeline()
	})

	go runForever(func() {
		s.runPubSub()
	})

	return nil
}

func (s *shard) runPublishPipeline() {
	var prs []pubRequest

	pingTicker := time.NewTicker(time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-pingTicker.C:
			conn := s.pool.Get()
			err := conn.Send("PUBLISH", "pingchannel", nil)
			if err != nil {
				s.node.logger.log(NewLogEntry(LogLevelError, "error publishing to ping channel", map[string]interface{}{"error": err.Error()}))
				err := conn.Close()
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
				}
				return
			}
			err = conn.Close()
			if err != nil {
				s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
			}

		case p := <-s.pubMessages:

			prs = append(prs, p)

		loop:
			for len(prs) < 512 {
				select {
				case pr := <-s.pubMessages:
					prs = append(prs, pr)
				default:
					break loop

				}
			}
			conn := s.pool.Get()
			for i := range prs {
				err := conn.Send("PUBLISH", prs[i].chId, prs[i].data)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error publishing data to redis", map[string]interface{}{"error": err.Error()}))
				}
				prs[i].done(nil)
			}
			err := conn.Flush()
			if err != nil {
				for i := range prs {
					prs[i].done(err)
				}
				err := conn.Close()
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
				}
				return
			}
			err = conn.Close()
			if err != nil {
				s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
			}
			prs = nil
		}
	}
}

func (s *shard) runPubSub() {
	conn := s.pool.Get()
	if conn == nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error connecting to redis: "))
		return
	}
	if conn.Err() != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error initializing redis connection", map[string]interface{}{"error": conn.Err().Error()}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	psc := &redis.PubSubConn{
		Conn: conn,
	}

	err := psc.Subscribe("pingchannel")
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error subscribing to ping channel", map[string]interface{}{"error": err.Error()}))
	}

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDoneOnce := func() {
		doneOnce.Do(func() {
			close(done)
		})
	}
	defer closeDoneOnce()

	// handling sub/unsub requests
	go func() {
		for {
			select {
			case <-done:
				err := psc.Close()
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
				}
				return
			case r := <-s.subCh:
				isSubscribe := r.subscribe
				channelBatch := []subRequest{r}

				chIDs := make([]interface{}, 0, len(r.channels))
				for _, ch := range r.channels {
					chIDs = append(chIDs, ch)
				}

				var otherR *subRequest

			loop:

				for len(chIDs) < 512 {
					select {
					case r := <-s.subCh:
						if r.subscribe != isSubscribe {
							otherR = &r
							break loop
						}
						channelBatch = append(channelBatch, r)
						for _, ch := range r.channels {
							chIDs = append(chIDs, ch)
						}
					default:
						break loop
					}
				}

				var opErr error
				if isSubscribe {
					opErr = psc.Subscribe(chIDs...)
				} else {
					opErr = psc.Unsubscribe(chIDs...)
				}

				if opErr != nil {
					for _, r := range channelBatch {
						r.done(opErr)
					}
					if otherR != nil {
						otherR.done(opErr)
					}
					err := psc.Close()
					if err != nil {
						s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
					}
					return
				}
				for _, r := range channelBatch {
					r.done(nil)
				}

				if otherR != nil {
					chIDs := make([]interface{}, 0, len(otherR.channels))
					for _, ch := range otherR.channels {
						chIDs = append(chIDs, ch)
					}
					var opErr error
					if otherR.subscribe {
						opErr = psc.Subscribe(chIDs...)
					} else {
						opErr = psc.Unsubscribe(chIDs...)
					}
					if opErr != nil {
						otherR.done(opErr)
						err := psc.Close()
						if err != nil {
							s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
						}
						return
					}
					otherR.done(nil)
				}
			}
		}
	}()

	// broadcasting new messages
	for i := 0; i < 15; i++ {
		go func() {
			for {
				select {
				case message := <-s.subMessages:

					switch ChannelID(message.Channel) {
					case "ping":
					case "node-info":

						s.node.handleNodeInfo(message.Data)

					default:

						var push clientproto.Event
						err := push.Unmarshal(message.Data)
						if err != nil {
							s.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling push from msg broker", map[string]interface{}{"channel": message.Channel, "error": err.Error()}))
						}

						appKey, channelName := parseChId(message.Channel)

						switch push.Type {
						case clientproto.EventType_PUBLICATION:

							var pub clientproto.Publication
							err := pub.Unmarshal(push.Data)
							if err != nil {
								s.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling publication from msg broker", map[string]interface{}{"channel": message.Channel, "error": err.Error()}))
							}

							s.node.hub.BroadcastPublication(appKey, channelName, &pub)

						case clientproto.EventType_JOIN:

							var join clientproto.Join
							err := join.Unmarshal(push.Data)
							if err != nil {
								s.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling join from msg broker", map[string]interface{}{"channel": message.Channel, "error": err.Error()}))
							}

							s.node.hub.BroadcastJoin(appKey, &join)

						case clientproto.EventType_LEAVE:

							var leave clientproto.Leave
							err := leave.Unmarshal(push.Data)
							if err != nil {
								s.node.logger.log(NewLogEntry(LogLevelError, "error unmarshaling leave from msg broker", map[string]interface{}{"channel": message.Channel, "error": err.Error()}))
							}

							s.node.hub.BroadcastLeave(appKey, &leave)
						}
					}
				}
			}
		}()
	}

	go func() {
		chIDs := make([]string, 2)
		chIDs[0] = "pingchannel"
		chIDs[1] = "--emitted-node-info"

		for _, ch := range s.node.hub.Channels() {
			if s.broker.getShard(ch) == s {
				chIDs = append(chIDs, ch)
			}
		}

		batch := make([]string, 0)

		for i, ch := range chIDs {
			if len(batch) > 0 && i%512 == 0 {
				r := newSubRequest(batch, true)
				err := s.sendSubRequest(r)
				if err != nil {
					closeDoneOnce()
					return
				}
				batch = nil
			}
			batch = append(batch, ch)
		}
		if len(batch) > 0 {
			r := newSubRequest(batch, true)
			err := s.sendSubRequest(r)
			if err != nil {
				closeDoneOnce()
				return
			}
		}
	}()

	// listening for new subMessages from pub/sub
	for {
		switch m := psc.ReceiveWithTimeout(10 * time.Second).(type) {

		case redis.Subscription:
		case redis.Message:
			s.subMessages <- m
		}
	}
}

func (s *shard) RunDataPipeline() {

	conn := s.pool.Get()

	err := s.addPresenceScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading add presence script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.remPresenceScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading remove presence script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.presenceScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading presence script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.getPresenceScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading get presence script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.updateStatsScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading update stats script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.retrieveStatsScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading stats script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.channelsScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading channels script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.addChannelScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading add channel script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.remChannelScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading remove channel script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = s.countChannelsScript.Load(conn)
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error loading count channels script", map[string]interface{}{"error": err}))
		err := conn.Close()
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
		}
		return
	}

	err = conn.Close()
	if err != nil {
		s.node.logger.log(NewLogEntry(LogLevelError, "error closing connection", map[string]interface{}{"error": err.Error()}))
	}

	var drs []dataRequest

	for dr := range s.dataMessages {
		drs = append(drs, dr)
	loop:
		for len(drs) < 512 {
			select {
			case dr := <-s.dataMessages:
				drs = append(drs, dr)
			default:
				break loop
			}
		}

		conn := s.pool.Get()

		for i := range drs {
			switch drs[i].op {
			case dataOpAddPresence:
				err := s.addPresenceScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "add presence", "error": err.Error()}))
				}
			case dataOpRemovePresence:
				err := s.remPresenceScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "remove presence", "error": err.Error()}))
				}
			case dataOpPresence:
				err := s.presenceScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "presence", "error": err.Error()}))
				}
			case dataOpGetPresence:
				err := s.getPresenceScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "get presence", "error": err.Error()}))
				}
			case dataOpUpdateStats:
				err := s.updateStatsScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "update stats", "error": err.Error()}))
				}
			case dataOpRetrieveStats:
				err := s.retrieveStatsScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "stats", "error": err.Error()}))
				}
			case dataOpChannels:
				err := s.channelsScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "update stats", "error": err.Error()}))
				}
			case dataOpAddChannel:
				err := s.addChannelScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "update stats", "error": err.Error()}))
				}
			case dataOpRemChannel:
				err := s.remChannelScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "update stats", "error": err.Error()}))
				}
			case dataOpCountChannels:
				err := s.countChannelsScript.SendHash(conn, drs[i].args...)
				if err != nil {
					s.node.logger.log(NewLogEntry(LogLevelError, "error executing redis script", map[string]interface{}{"script": "update stats", "error": err.Error()}))
				}
			}
		}

		err := conn.Flush()

		if err != nil {
			for i := range drs {
				drs[i].done(nil, err)
			}
			s.node.logger.log(NewLogEntry(LogLevelError, "error flushing data pipeline", map[string]interface{}{"error": err.Error()}))
		}

		var noScriptError bool
		for i := range drs {
			reply, err := conn.Receive()
			if err != nil {
				if e, ok := err.(redis.Error); ok && strings.HasPrefix(string(e), "NOSCRIPT ") {
					noScriptError = true
				}
			}
			drs[i].done(reply, err)
		}
		if noScriptError {
			// Start this func from the beginning and LOAD missing script.
			conn.Close()
			return
		}
		conn.Close()
		drs = nil
	}
}

func (s *shard) Subscribe(channels []string) error {
	sub := newSubRequest(channels, true)
	return s.sendSubRequest(sub)
}

func (s *shard) Unsubscribe(channels []string) error {
	sub := newSubRequest(channels, false)
	return s.sendSubRequest(sub)
}

func (s *shard) handlePublish(chId string, clientInfo *clientproto.ClientInfo, r *clientproto.PublishRequest) error {

	pub := &clientproto.Publication{
		Topic:   r.Topic,
		Channel: r.Channel,
		Data:    r.Data,
		Info:    clientInfo,
	}

	return s.Publish(chId, pub)

}

func (s *shard) handleSubscribe(chId string, uid string, clientInfo *clientproto.ClientInfo, r *clientproto.SubscribeRequest) error {

	join := &clientproto.Join{
		Channel: r.Channel,
		Data:    clientInfo,
	}

	return s.PublishJoin(chId, join)
}

func (s *shard) handleUnsubscribe(chId string, uid string, clientInfo *clientproto.ClientInfo, r *clientproto.UnsubscribeRequest) error {

	leave := &clientproto.Leave{
		Channel: r.Channel,
		Data:    clientInfo,
	}

	return s.PublishLeave(chId, leave)
}

/*
|---------------------------------------------------------
|	Publishers
|---------------------------------------------------------
|
|
*/

func (s *shard) Publish(chId string, publication *clientproto.Publication) error {
	eChan := make(chan error, 1)

	bytes, err := publication.Marshal()
	if err != nil {
		return err
	}

	packet := &clientproto.Event{
		Type: clientproto.EventType_PUBLICATION,
		Data: bytes,
	}

	payload, err := packet.Marshal()
	if err != nil {
		return err
	}

	pr := pubRequest{
		chId: chId,
		data: payload,
		err:  eChan,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.SetTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return RedisWriteTimeoutError
		}
	}

	messagesSentCountPublication.Inc()

	return <-eChan
}

func (s *shard) PublishJoin(chId string, join *clientproto.Join) error {
	eChan := make(chan error, 1)

	bytes, err := join.Marshal()
	if err != nil {
		return err
	}

	packet := &clientproto.Event{
		Type: clientproto.EventType_JOIN,
		Data: bytes,
	}

	payload, err := packet.Marshal()
	if err != nil {
		return err
	}

	pr := pubRequest{
		chId: chId,
		data: payload,
		err:  eChan,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.SetTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return RedisWriteTimeoutError
		}
	}

	messagesSentCountJoin.Inc()

	return <-eChan
}

func (s *shard) PublishLeave(chId string, l *clientproto.Leave) error {
	eChan := make(chan error, 1)

	bytes, err := l.Marshal()
	if err != nil {
		return err
	}

	event := &clientproto.Event{
		Type: clientproto.EventType_LEAVE,
		Data: bytes,
	}

	payload, _ := event.Marshal()

	pr := pubRequest{
		chId: chId,
		data: payload,
		err:  eChan,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.SetTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return errors.New("redis timeout")
		}
	}

	messagesSentCountLeave.Inc()

	return <-eChan
}

func (s *shard) PublishNode(data []byte) error {
	eChan := make(chan error, 1)

	pr := pubRequest{
		chId: "--emitted-node-info",
		data: data,
		err:  eChan,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.SetTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return RedisWriteTimeoutError
		}
	}

	return <-eChan
}

/*
|-----------------------------------
|	Broker requests
|-----------------------------------
|
|
*/

type pubRequest struct {
	chId string
	data []byte
	err  chan error
}

func (pr *pubRequest) done(err error) {
	pr.err <- err
}

func (pr *pubRequest) result() error {
	return <-pr.err
}

func (s *shard) sendPubRequest(pub pubRequest) error {
	select {
	case s.pubMessages <- pub:
	default:
		timer := timers.SetTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pub:
		case <-timer.C:
			return RedisWriteTimeoutError
		}
	}

	return pub.result()
}

type subRequest struct {
	channels  []string
	subscribe bool
	err       chan error
}

func (sr *subRequest) done(err error) {
	sr.err <- err
}

func (sr *subRequest) result() error {
	return <-sr.err
}

func (s *shard) sendSubRequest(sub subRequest) error {
	select {
	case s.subCh <- sub:
	default:
		timer := timers.SetTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.subCh <- sub:
		case <-timer.C:
			return RedisWriteTimeoutError
		}
	}
	return sub.result()
}

func newSubRequest(channels []string, subscribe bool) subRequest {
	return subRequest{
		channels:  channels,
		subscribe: subscribe,
		err:       make(chan error, 1),
	}
}

type dataOp int

const (
	dataOpAddPresence dataOp = iota
	dataOpRemovePresence
	dataOpPresence
	dataOpGetPresence
	dataOpUpdateStats
	dataOpRetrieveStats
	dataOpChannels
	dataOpAddChannel
	dataOpRemChannel
	dataOpCountChannels
)

type dataResponse struct {
	reply interface{}
	err   error
}

type dataRequest struct {
	op   dataOp
	args []interface{}
	resp chan *dataResponse
}

func newDataRequest(op dataOp, args []interface{}) dataRequest {
	return dataRequest{op: op, args: args, resp: make(chan *dataResponse, 1)}
}

func (dr *dataRequest) done(reply interface{}, err error) {
	if dr.resp == nil {
		return
	}
	dr.resp <- &dataResponse{reply: reply, err: err}
}

func (dr *dataRequest) result() *dataResponse {
	if dr.resp == nil {
		// No waiting, as caller didn't care about response.
		return &dataResponse{}
	}
	return <-dr.resp
}

package main

import (
	"errors"
	"github.com/sireax/emitted/internal/timers"
	"log"
	"net"
	"runtime"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Broker structure represents message broker, connected with API
type Broker struct {
	node   *Node
	shards []*shard
	config BrokerConfig
}

type BrokerConfig struct {
	Shards []BrokerShardConfig
}

type shard struct {
	node        *Node
	broker      *Broker
	config      BrokerShardConfig
	pool        *redis.Pool
	subCh       chan *subRequest
	subMessages chan redis.Message
	pubMessages chan pubRequest
}

type BrokerShardConfig struct {
	Host string
	// Port is Redis server port.
	Port int
	// Password is password to use when connecting to Redis database. If empty then password not used.
	Password string
	// DB is Redis database number. If not set then database 0 used.
	DB int
	// MasterName is a name of Redis instance master Sentinel monitors.
	MasterName string
	// IdleTimeout is timeout after which idle connections to Redis will be closed.
	IdleTimeout time.Duration
	// PubSubNumWorkers sets how many PUB/SUB message processing workers will be started.
	// By default we start runtime.NumCPU() workers.
	PubSubNumWorkers int
	// ReadTimeout is a timeout on read operations. Note that at moment it should be greater
	// than node ping publish interval in order to prevent timing out Pubsub connection's
	// Receive call.
	ReadTimeout time.Duration
	// WriteTimeout is a timeout on write operations.
	WriteTimeout time.Duration
	// ConnectTimeout is a timeout on connect operation.
	ConnectTimeout time.Duration
}

// NewBroker function initializes BrokerMessage Broker
func NewBroker(n *Node, config BrokerConfig) (*Broker, error) {

	var shards []*shard

	if len(config.Shards) == 0 {
		return nil, errors.New("no Redis shards provided in configuration")
	}

	if len(config.Shards) > 1 {
		log.Println("sharding", len(config.Shards), "instances")
	}

	for _, conf := range config.Shards {
		shard, err := newShard(n, conf)
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

func (b *Broker) Subscribe(chId string) {
	b.getShard(chId).Subscribe([]string{chId})
}

func (b *Broker) Unsubscribe(chId string) {
	b.getShard(chId).Unsubscribe([]string{chId})
}

func (b *Broker) Publish(ch string, c *Client, p *PublishRequest) error {
	return b.getShard(ch).handlePublish(ch, c, p)
}

func (b *Broker) HandleSubscribe(chId string, r *SubscribeRequest, p *ClientInfo) error {
	return b.getShard(chId).handleSubscribe(chId, r, p)
}

func (b *Broker) HandleUnsubscribe(chId string, r *UnsubscribeRequest, p *ClientInfo) error {
	return b.getShard(chId).handleUnsubscribe(chId, r, p)
}

func (b *Broker) PublishJoin(chId string, join *Join) error {
	return b.getShard(chId).PublishJoin(chId, join)
}

func (b *Broker) PublishLeave(chId string, leave *Leave) error {
	return b.getShard(chId).PublishLeave(chId, leave)
}

func newPool(n *Node, conf BrokerShardConfig) *redis.Pool {
	host := conf.Host
	port := conf.Port
	password := conf.Password
	db := conf.DB

	serverAddr := net.JoinHostPort(host, strconv.Itoa(port))

	poolSize := runtime.NumCPU()

	maxIdle := 64
	if poolSize < maxIdle {
		maxIdle = poolSize
	}

	return &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   poolSize,
		Wait:        true,
		IdleTimeout: conf.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			var err error

			var readTimeout = time.Second
			if conf.ReadTimeout != 0 {
				readTimeout = conf.ReadTimeout
			}
			var writeTimeout = time.Second
			if conf.WriteTimeout != 0 {
				writeTimeout = conf.WriteTimeout
			}
			var connectTimeout = time.Second * 30
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

func newShard(n *Node, conf BrokerShardConfig) (*shard, error) {
	shard := &shard{
		node:   n,
		config: conf,
		pool:   newPool(n, conf),
	}
	shard.pubMessages = make(chan pubRequest)
	shard.subCh = make(chan *subRequest)
	shard.subMessages = make(chan redis.Message)
	return shard, nil
}

func (b *Broker) getShard(channel string) *shard {
	return b.shards[consistentIndex(channel, len(b.shards))]
}

func (s *shard) Run() error {

	s.pool = &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000, // max number of connections
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", "localhost:6379")
			if err != nil {
				log.Fatalln("error while initializing redis pub/sub connection: ", err)
			}

			return conn, nil
		},
	}

	go runForever(func() {
		s.runPublishPipeline()
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
			err := conn.Send("PUBLISH", "PING_TEST_CHANNEL", nil)
			if err != nil {
				log.Fatal(err)
				conn.Close()
				return
			}
			conn.Close()

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
				conn.Send("PUBLISH", prs[i].chId, prs[i].data)
			}
			err := conn.Flush()
			if err != nil {
				for i := range prs {
					prs[i].done(err)
				}
				conn.Close()
				return
			}
			conn.Close()
			prs = nil
		}
	}

}

func (s *shard) runPubSub() {
	conn := s.pool.Get()

	psc := &redis.PubSubConn{
		Conn: conn,
	}

	err := psc.Subscribe("PING_TEST_CHANNEL")
	if err != nil {
		log.Fatalln("Something wrong with the ping channel: ", err)
	}

	// handling sub/unsub requests
	go func() {
		for {
			select {
			case r := <-s.subCh:
				if r.subscribe == true {
					for _, channel := range r.channels {
						err := psc.Subscribe(channel)
						if err != nil {
							log.Fatal("error while subscribing to channel: ", err)
						}
					}
				} else {
					for _, channel := range r.channels {
						err := psc.Unsubscribe(channel)

						if err != nil {
							log.Fatal("error while unsubscribing from channel: ", err)
						}
					}
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
					default:

						var push Push
						err := push.Unmarshal(message.Data)
						if err != nil {
							log.Fatal(err)
						}

						appKey, channelName := parseChId(message.Channel)

						switch push.Type {
						case PushTypePublication:
							var pub Publication
							err := pub.Unmarshal(push.Data)
							if err != nil {
								continue
							}

							node.hub.BroadcastMessage(appKey, channelName, &pub)

						case PushTypeJoin:
							var join Join
							err := join.Unmarshal(push.Data)
							if err != nil {
								log.Fatal(err)
							}

							node.hub.BroadcastJoin(appKey, &join)
						case PushTypeLeave:
							var leave Leave
							err := leave.Unmarshal(push.Data)
							if err != nil {
								log.Fatal(err)
							}

							node.hub.BroadcastLeave(appKey, &leave)
						}
					}
				}
			}
		}()
	}

	// listening for new subMessages from pub/sub
	for {
		switch m := psc.ReceiveWithTimeout(10 * time.Second).(type) {

		case redis.Subscription:
		case redis.Message:
			s.subMessages <- m
		}
	}
}

// HandleSubscribe ...
func (s *shard) Subscribe(channels []string) {
	sub := newSubRequest(channels, true)
	s.sendSubRequest(sub)
}

// HandleUnsubscribe ...
func (s *shard) Unsubscribe(channels []string) {
	sub := newSubRequest(channels, false)
	s.sendSubRequest(sub)
}

func (s *shard) handlePublish(chId string, c *Client, p *PublishRequest) error {

	publication := &Publication{
		Data: p.Data,
		Info: &ClientInfo{
			Id: "random user",
		},
	}

	return s.Publish(chId, publication)

}

func (s *shard) handleSubscribe(chId string, r *SubscribeRequest, info *ClientInfo) error {

	join := &Join{
		Channel: r.Channel,
	}

	if info != nil {
		join.Data = info
	}

	return s.PublishJoin(chId, join)
}

func (s *shard) handleUnsubscribe(chId string, r *UnsubscribeRequest, info *ClientInfo) error {

	leave := &Leave{
		Channel: r.Channel,
	}

	if info != nil {
		leave.Info = info
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

func (s *shard) Publish(chId string, publication *Publication) error {

	bytes, err := publication.Marshal()
	if err != nil {
		return err
	}

	push := &Push{
		Type: PushTypePublication,
		Data: bytes,
	}

	payload, err := push.Marshal()
	if err != nil {
		return err
	}

	pr := pubRequest{
		chId: chId,
		data: payload,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.AcquireTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return errors.New("redis timeout")
		}
	}

	return nil
}

func (s *shard) PublishJoin(chId string, join *Join) error {

	bytes, err := join.Marshal()
	if err != nil {
		return err
	}

	push := &Push{
		Type: PushTypeJoin,
		Data: bytes,
	}

	payload, err := push.Marshal()
	if err != nil {
		return err
	}

	pr := pubRequest{
		chId: chId,
		data: payload,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.AcquireTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return errors.New("redis timeout")
		}
	}

	return nil
}

func (s *shard) PublishLeave(chId string, leave *Leave) error {

	bytes, err := leave.Marshal()
	if err != nil {
		return err
	}

	push := &Push{
		Type: PushTypeLeave,
		Data: bytes,
	}

	payload, _ := push.Marshal()

	pr := pubRequest{
		chId: chId,
		data: payload,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.AcquireTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return errors.New("redis timeout")
		}
	}
	return nil
}

func (s *shard) PublishNode(data []byte) error {

	pr := pubRequest{
		chId: "sysnodeinfo",
		data: data,
	}

	select {
	case s.pubMessages <- pr:
	default:
		timer := timers.AcquireTimer(time.Second)
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubMessages <- pr:
		case <-timer.C:
			return errors.New("redis timeout reached")
		}
	}

	return nil
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
}

func (r pubRequest) done(err error) {

}

type subRequest struct {
	channels  []string
	subscribe bool
}

func (s *shard) sendSubRequest(sub *subRequest) {
	s.subCh <- sub
	return
}

func newSubRequest(channels []string, subscribe bool) *subRequest {
	return &subRequest{
		channels:  channels,
		subscribe: subscribe,
	}
}

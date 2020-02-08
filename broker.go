package main

import (
	"errors"
	"github.com/sireax/emitted/internal/timers"
	"log"
	"net"
	"strconv"
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
	subCh       chan subRequest
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

func (b *Broker) Subscribe(chId string) error {
	return b.getShard(chId).Subscribe([]string{chId})
}

func (b *Broker) Unsubscribe(chId string) {
	b.getShard(chId).Unsubscribe([]string{chId})
}

func (b *Broker) Publish(ch string, cInfo *ClientInfo, p *PublishRequest) error {
	return b.getShard(ch).handlePublish(ch, cInfo, p)
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
	shard.subCh = make(chan subRequest)
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
			err := conn.Send("PUBLISH", "pingchannel", nil)
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
				prs[i].done(nil)
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
	if conn.Err() != nil {
		conn.Close()
		return
	}

	psc := &redis.PubSubConn{
		Conn: conn,
	}

	err := psc.Subscribe("pingchannel")
	if err != nil {
		log.Fatalln("Something wrong with the ping channel: ", err)
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
				psc.Close()
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
					psc.Close()
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
						psc.Close()
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

	go func() {
		chIDs := make([]string, 2)
		chIDs[0] = "pingchannel"
		chIDs[1] = "sysnodeinfo"

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

// HandleSubscribe ...
func (s *shard) Subscribe(channels []string) error {
	sub := newSubRequest(channels, true)
	return s.sendSubRequest(sub)
}

// HandleUnsubscribe ...
func (s *shard) Unsubscribe(channels []string) error {
	sub := newSubRequest(channels, false)
	return s.sendSubRequest(sub)
}

func (s *shard) handlePublish(chId string, cInfo *ClientInfo, p *PublishRequest) error {

	pub := &Publication{
		Data: p.Data,
	}

	if cInfo != nil {
		pub.Info = cInfo
	}

	return s.Publish(chId, pub)

}

func (s *shard) handleSubscribe(chId string, r *SubscribeRequest, info *ClientInfo) error {

	join := &Join{
		Channel: r.Channel,
	}

	if len(info.Info) > 0 {
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
	eChan := make(chan error, 1)

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

	return <-eChan
}

func (s *shard) PublishJoin(chId string, join *Join) error {
	log.Println("publish handled")
	eChan := make(chan error, 1)

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

	return <-eChan
}

func (s *shard) PublishLeave(chId string, leave *Leave) error {
	eChan := make(chan error, 1)

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
	return <-eChan
}

func (s *shard) PublishNode(data []byte) error {
	eChan := make(chan error, 1)

	pr := pubRequest{
		chId: "sysnodeinfo",
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
			return errors.New("redis timeout reached")
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
			return errors.New("redis timeout")

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
			return errors.New("redis timeout")
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

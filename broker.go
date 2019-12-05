package main

import (
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Broker structure represents message broker, connected with API
type Broker struct {
	pool     *redis.Pool
	Config   *BrokerConfig
	subCh    chan *subRequest
	messages chan redis.Message
}

// NewBroker function initializes Message Broker
func NewBroker() *Broker {
	return &Broker{
		Config:   config.Broker,
		subCh:    make(chan *subRequest, 0),
		messages: make(chan redis.Message, 0),
	}
}

// Run ...
func (b *Broker) Run() {

	b.pool = &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000, // max number of connections
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", b.Config.Host+":"+b.Config.Port)
			if err != nil {
				log.Fatalln("error while initializing redis pub/sub connection: ", err)
			}

			return conn, nil
		},
	}

	go b.runPublishPipeline()

	time.Sleep(time.Second)

	go b.runPubSub()
}

func (b *Broker) runPublishPipeline() {

	pingTicker := time.NewTicker(time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-pingTicker.C:
			conn := b.pool.Get()
			err := conn.Send("PUBLISH", "PING_TEST_CHANNEL", nil)
			if err != nil {
				log.Fatal(err)
				conn.Close()
				return
			}
			conn.Close()
		}
	}

}

// ChannelID ...
func ChannelID(key string) string {
	switch key {
	case "PING_TEST_CHANNEL":
		return "ping"
	default:
		return key
	}
}

func (b *Broker) runPubSub() {
	conn := b.pool.Get()

	psc := &redis.PubSubConn{
		Conn: conn,
	}

	psc.Subscribe("PING_TEST_CHANNEL")

	// these workers will add new subscriptions
	go func() {
		for {
			select {
			case r := <-b.subCh:
				if r.subscribe == true {
					for _, tunnel := range r.tunnels {
						err := psc.Subscribe(tunnel)
						if err != nil {
							log.Fatal("error while subscribing to channel: ", err)
						}
					}
				} else {
					for _, tunnel := range r.tunnels {
						err := psc.Unsubscribe(tunnel)
						if err != nil {
							log.Fatal("error while unsubscribing from channel: ", err)
						}
					}
				}
			}
		}
	}()

	// these workers will broadcast new messages
	for i := 0; i < b.Config.PubSubWorkers; i++ {
		log.Println("worker", i, "is up and running")
		go func() {
			for {
				select {
				case message := <-b.messages:
					switch ChannelID(message.Channel) {
					case "ping":
					default:
						tunnel := message.Channel

						var publication Publication
						publication.Tunnel = message.Channel
						publication.Data = message.Data

						hub.BroadcastMessage(tunnel, publication)
					}
				}
			}
		}()
	}

	// Adding current subscriptions to redis pub/sub
	go func() {
		tunnels := hub.Tunnels()

		if len(tunnels) > 0 {
			sub := newSubRequest(tunnels, true)
			b.sendSubRequest(sub)
		}

	}()

	// listening for new messages from pub/sub
	go func() {
		for {
			switch m := psc.ReceiveWithTimeout(10 * time.Second).(type) {
			case redis.Message:
				b.messages <- m
			case redis.Subscription:
				log.Println("New subscription: ", m.Channel)
			}
		}
	}()
}

// Subscribe ...
func (b *Broker) Subscribe(tunnels []string) {
	sub := newSubRequest(tunnels, true)
	b.sendSubRequest(sub)
}

// Unsubscribe ...
func (b *Broker) Unsubscribe(tunnels []string) {
	sub := newSubRequest(tunnels, false)
	b.sendSubRequest(sub)
}

// SubRequest helps to subscribe tunnels by transferring them into a slice
// it is stored in Broker.subCh, which is listened by workers
type subRequest struct {
	tunnels   []string
	subscribe bool
}

func (b *Broker) sendSubRequest(sub *subRequest) {
	b.subCh <- sub
	return
}

// NewSubRequest ...
func newSubRequest(tunnels []string, subscribe bool) *subRequest {
	return &subRequest{
		tunnels:   tunnels,
		subscribe: subscribe,
	}
}

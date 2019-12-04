package main

import (
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Broker structure represents message broker, connected with API
type Broker struct {
	redis  *redis.PubSubConn
	Config *BrokerConfig
	subCh  chan *subRequest
}

// NewBroker function initializes Message Broker
func NewBroker() *Broker {
	return &Broker{
		Config: config.Broker,
	}
}

// Run broker node
func (b *Broker) Run() {
	conn, err := redis.Dial("tcp", b.Config.Host+":"+b.Config.Port)
	if err != nil {
		log.Fatal(err)
	}
	psc := &redis.PubSubConn{
		Conn: conn,
	}
	b.redis = psc

	// these workers will add new subscriptions
	go func() {
		for {
			select {
			case r := <-b.subCh:
				if r.subscribe == true {
					for _, tunnel := range r.tunnels {
						psc.Subscribe(tunnel)
						log.Println("Subscribed to", tunnel)
					}
				} else {
					for _, tunnel := range r.tunnels {
						psc.Unsubscribe(tunnel)
						log.Println("Unsubscribed from", tunnel)
					}
				}
			}
		}
	}()

	// these workers will broadcast new messages
	stream := make(chan redis.Message)
	for i := 0; i < b.Config.PubSubWorkers; i++ {
		log.Println("worker", i, "is up and running")
		go func() {
			for {
				select {
				case message := <-stream:
					tunnel := message.Channel

					var publication Publication
					publication.Tunnel = message.Channel
					publication.Unmarshal(message.Data)
					log.Println(publication)

					hub.BroadcastMessage(tunnel, publication)
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
			if err != nil {
				log.Fatal(err)
			}
		}

	}()

	// listening for new messages from pub/sub
	go func() {
		for {
			switch m := psc.ReceiveWithTimeout(10 * time.Second).(type) {
			case redis.Message:
				stream <- m
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
	go func() {
		b.subCh <- sub
	}()
}

// NewSubRequest ...
func newSubRequest(tunnels []string, subscribe bool) *subRequest {
	return &subRequest{
		tunnels:   tunnels,
		subscribe: subscribe,
	}
}

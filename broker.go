package main

import (
	"log"
	"strings"
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

func (b *Broker) Run() error {

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

	return nil
}

func (b *Broker) Publish(app *App, channel *Channel, data []byte)  {
	b.pool.Get().Do("PUBLISH", channel.App.Key + ":" + channel.Name, data)
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
					for _, channel := range r.channels {
						log.Println(channel)
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

						arr := strings.Split(message.Channel, ":")
						appKey := arr[0]
						channelName := arr[1]

						node.hub.BroadcastMessage(appKey, channelName, message.Data)
					}
				}
			}
		}()
	}

	// Adding current subscriptions to redis pub/sub
	// go func() {
	// 	channels := hub.Channels()
	//
	// 	if len(channels) > 0 {
	// 		sub := newSubRequest(channels, true)
	// 		b.sendSubRequest(sub)
	// 	}
	//
	// }()

	// listening for new messages from pub/sub
	go func() {
		for {
			switch m := psc.ReceiveWithTimeout(10 * time.Second).(type) {
			case redis.Message:
				b.messages <- m
			case redis.Subscription:
			}
		}
	}()
}

// Subscribe ...
func (b *Broker) Subscribe(channels []string) {
	sub := newSubRequest(channels, true)
	b.sendSubRequest(sub)
}

// Unsubscribe ...
func (b *Broker) Unsubscribe(channels []string) {
	sub := newSubRequest(channels, false)
	b.sendSubRequest(sub)
}

// SubRequest helps to subscribe channels by transferring them into a slice
// it is stored in Broker.subCh, which is listened by workers
type subRequest struct {
	channels  []string
	subscribe bool
}

func (b *Broker) sendSubRequest(sub *subRequest) {
	b.subCh <- sub
	return
}

// NewSubRequest ...
func newSubRequest(channels []string, subscribe bool) *subRequest {
	return &subRequest{
		channels:  channels,
		subscribe: subscribe,
	}
}

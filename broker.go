package main

import (
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Broker structure represents message broker, connected with API
type Broker struct {
	pool        *redis.Pool
	Config      *BrokerConfig
	subCh       chan *subRequest
	subMessages chan redis.Message
	pubMessages chan pubRequest
}

// NewBroker function initializes BrokerMessage Broker
func NewBroker() *Broker {
	return &Broker{
		Config:      config.Broker,
		subCh:       make(chan *subRequest, 0),
		subMessages: make(chan redis.Message, 0),
		pubMessages: make(chan pubRequest, 0),
	}
}

/*
|
|
|--------------------------------------------------
|	Run broker
|--------------------------------------------------
|
|
 */

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

	go runForever(func() {
		b.runPublishPipeline()
	})

	go runForever(func() {
		b.runPubSub()
	})

	return nil
}

/*
|
|
|---------------------------------------------------
|	Publish pipeline
|---------------------------------------------------
|
|
 */

func (b *Broker) runPublishPipeline() {
	var prs []pubRequest

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

		case msg := <-b.pubMessages:
			prs = append(prs, msg)

		loop:

			for len(prs) < 512 {
				select {
				case msg := <- b.pubMessages:
					prs = append(prs, msg)
				default:
					break loop
				}

				conn := node.broker.pool.Get()
				for i := range prs {
					err := conn.Send("PUBLISH", prs[i].chId, prs[i].data)
					if err != nil {
						log.Println("Failed publishing message: ", err)
					}
				}
			}
		}
	}

}

/*
|
|
|-------------------------------------------------
|	Publish / Subscribe goroutines
|-------------------------------------------------
|
|
 */

func (b *Broker) runPubSub() {
	conn := b.pool.Get()

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

	// broadcasting new messages
	for i := 0; i < b.Config.PubSubWorkers; i++ {
		log.Println("message worker", i, "is up and running")
		go func() {
			for {
				select {
				case message := <-b.subMessages:

					switch ChannelID(message.Channel) {
					case "ping":
					default:

						var push Push
						err := push.Unmarshal(message.Data)
						if err != nil {
							continue
						}

						appKey, channelName := parseChId(message.Channel)

						switch push.Type {
						case PushTypePublication:
							var pub Publication
							err := pub.Unmarshal(pub.Data)
							if err != nil {
								continue
							}
							node.hub.BroadcastMessage(appKey, channelName, &pub)
						case PushTypeJoin:
							var join Join
							err := join.Unmarshal(push.Data)
							if err != nil {
								continue
							}
							node.hub.BroadcastJoin(appKey, &join)
						case PushTypeLeave:
							var leave Leave
							err := leave.Unmarshal(push.Data)
							if err != nil {
								continue
							}
							node.hub.BroadcastLeave(appKey, &leave)
						}
					}
				}
			}
		}()
	}

	// listening for new subMessages from pub/sub
	go func() {
		for {
			switch m := psc.ReceiveWithTimeout(10 * time.Second).(type) {
			case redis.Message:
				b.subMessages <- m
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

/*
|
|
|--------------------------------------------------------
|	Client handlers
|--------------------------------------------------------
|
|
 */

func (b *Broker) handlePublish(chId string, p *PublishRequest) {

	push := &Push{
		Type: PushTypePublication,
		Data: p.Data,
	}

	bytesData, err := push.Marshal()
	if err != nil {
		return
	}

	pr := pubRequest{
		chId: chId,
		data: bytesData,
	}

	select {
	case b.pubMessages <- pr:
	default:

	}

}

func (b *Broker) handleSubscribe(chId string, r *SubscribeRequest)  {

	join := &Join{
		Channel: r.Channel,
	}

	bytesData, err := join.Marshal()
	if err != nil {
		return
	}

	push := &Push{
		Type: PushTypeJoin,
		Data: bytesData,
	}

	bytePush, err := push.Marshal()
	if err != nil {
		return
	}

	pr := pubRequest{
		chId: chId,
		data: bytePush,
	}

	select {
	case b.pubMessages <- pr:
	default:

	}
}

func (b *Broker) handleUnsubscribe(chId string, r *UnsubscribeRequest)  {

	leave := &Leave{
		Channel: r.Channel,
	}

	bytesData, err := leave.Marshal()
	if err != nil {
		return
	}

	push := &Push{
		Type: PushTypeLeave,
		Data: bytesData,
	}

	bytesPush, err := push.Marshal()
	if err != nil {
		return
	}

	pr := pubRequest{
		chId: chId,
		data: bytesPush,
	}

	select {
	case b.pubMessages <- pr:
	default:

	}

}


/*
|
|
|---------------------------------------------------------
|	Broker requests
|---------------------------------------------------------
|
|
 */

type pubRequest struct {
	chId string
	data []byte
}

type subRequest struct {
	channels  []string
	subscribe bool
}

func (b *Broker) sendSubRequest(sub *subRequest) {
	b.subCh <- sub
	return
}

func newSubRequest(channels []string, subscribe bool) *subRequest {
	return &subRequest{
		channels:  channels,
		subscribe: subscribe,
	}
}

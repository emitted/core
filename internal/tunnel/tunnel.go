package tunnel

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	broker "github.com/sireax/emitted/internal/broker"
	p "github.com/sireax/emitted/internal/packet"
)

var br = broker.NewBroker()

// Tunnel structure represents an instance
// of the channel, that clientis connected to.
type Tunnel struct {
	APIkey         string
	Key            string `json:"seckey"`
	Subscribers    map[*websocket.Conn]bool
	Broadcast      chan *p.Packet
	Connected      int
	MaxConnections int
	Mux            sync.Mutex
}

// NewTunnel is a constructor for Tunnel structure
func NewTunnel(APIKey string, key string, maxConnections int) *Tunnel {
	tunnel := &Tunnel{
		APIkey:         APIKey,
		Key:            key,
		Subscribers:    make(map[*websocket.Conn]bool),
		Broadcast:      make(chan *p.Packet),
		Connected:      0,
		MaxConnections: maxConnections,
	}
	go tunnel.Run(br)
	return tunnel
}

// GetIfExists ...
func GetIfExists(key string) (*Tunnel, error) {
	results, _ := br.Redis.SInter("tunnels:maps:" + key).Result()
	if len(results) == 0 {
		return nil, errors.New("tunnel does not exist")
	}

	model, _ := br.Redis.HVals("tunnels:" + results[0]).Result()
	if len(model) == 0 {
		return nil, errors.New("tunnel does not exist")
	}

	tunnel := NewTunnel(model[0], model[1], 100)
	return tunnel, nil
}

// ConnectSubscriber ...
func (t *Tunnel) ConnectSubscriber(client *websocket.Conn) {
	t.Mux.Lock()
	t.Subscribers[client] = true
	t.Connected++
	t.Mux.Unlock()

	data, _ := json.Marshal(`Authentication succeded`)
	client.WriteJSON(p.NewPacket("auth_succeded", data))
}

// DisconnectSubscriber ...
func (t *Tunnel) DisconnectSubscriber(client *websocket.Conn) {
	t.Mux.Lock()
	delete(t.Subscribers, client)
	t.Connected--
	t.Mux.Unlock()
}

// SubscribersConnected method counts concurrent connections
func (t *Tunnel) SubscribersConnected() int {
	return t.Connected
}

// ListenBroker function starts to listen Broker for messages
func (t *Tunnel) ListenBroker(b *broker.Broker) {
	sub := b.Redis.Subscribe(t.Key)
	for {
		select {
		case packet := <-sub.Channel():
			var message *p.Packet
			messageRaw := packet.Payload
			err := json.Unmarshal([]byte(messageRaw), &message)
			if err != nil {
				fmt.Println(err)
				break
			}
			t.Broadcast <- message
		}
	}
}

// BroadcastMessages ...
func (t *Tunnel) BroadcastMessages() {
	for {
		select {
		case packet := <-t.Broadcast:
			for client := range t.Subscribers {
				err := client.WriteJSON(packet)
				if err != nil {
					log.Printf("error: %v", err)
					client.Close()

				}
			}
		}
	}
}

// Run ...
func (t *Tunnel) Run(b *broker.Broker) {
	go t.ListenBroker(b)
	go t.BroadcastMessages()
}

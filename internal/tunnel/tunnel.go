package tunnel

import (
	"log"
	"sync"

	"github.com/gorilla/websocket"
	broker "github.com/sireax/Emmet-Go-Server/internal/broker"
	errs "github.com/sireax/Emmet-Go-Server/internal/errors"
)

var br = broker.NewBroker()

// Tunnel structure represents an instance
// of the channel, that clientis connected to.
type Tunnel struct {
	APIkey         string
	Key            string `json:"seckey"`
	Subscribers    map[*websocket.Conn]bool
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
		Connected:      0,
		MaxConnections: maxConnections,
	}
	return tunnel
}

// GetIfExists ...
func GetIfExists(key string) (*Tunnel, error) {
	results, _ := br.Redis.Get("tunnels:maps:" + key).Result()
	if results == "" {
		return nil, errs.NewErrTunnelNotFound()
	}

	model, _ := br.Redis.HVals("tunnels:" + results).Result()
	if len(model) == 0 {
		return nil, errs.NewErrTunnelNotFound()
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
}

// DisconnectSubscriber ...
func (t *Tunnel) DisconnectSubscriber(client *websocket.Conn) {
	t.Mux.Lock()
	delete(t.Subscribers, client)
	t.Connected--
	t.Mux.Unlock()
	log.Println("client disconnected")
}

// SubscribersConnected method counts concurrent connections
func (t *Tunnel) SubscribersConnected() int {
	return t.Connected
}

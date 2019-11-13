package hub

import (
	"encoding/json"
	"errors"

	"github.com/go-redis/redis"
	p "github.com/sireax/Emmet-Go-Server/internal/packet"
	t "github.com/sireax/Emmet-Go-Server/internal/tunnel"
)

// Hub struct contains all data and structs of clients,tunnels etc.
type Hub struct {
	Redis   *redis.Client
	Tunnels map[string]*t.Tunnel // tunnels map
}

// NewHub is a constructor method for the Hub struct
func NewHub() *Hub {
	return &Hub{
		Redis:   redis.NewClient(&redis.Options{}),
		Tunnels: make(map[string]*t.Tunnel),
	}
}

// FindTunnel ...
func (hub *Hub) FindTunnel(key string) (*t.Tunnel, error) {
	tunnel, ok := hub.Tunnels[key]
	if ok != true {
		return nil, errors.New("tunnel does not exist")
	}

	return tunnel, nil
}

// SetTunnel ...
func (hub *Hub) SetTunnel(key string, tunnel *t.Tunnel) {
	hub.Tunnels[key] = tunnel
}

// GetTunnel primarily searchs tunnel in map by key,
// if it does not exist it looks for it in redis
func (hub *Hub) GetTunnel(key string) (*t.Tunnel, error) {

	// Looking for tunnel in map
	tunnel, err := hub.FindTunnel(key)
	if err == nil {
		return tunnel, nil
	}

	// looking for tunnel in database
	tunnel, err = t.GetIfExists(key)
	if err != nil {
		return nil, err
	}

	hub.SetTunnel(key, tunnel)

	return tunnel, nil
}

// worker is a worker pattern
func (hub *Hub) worker(flow *redis.PubSub) {
	type RedisMessage struct {
		Tunnel string
		Data   json.RawMessage
	}

	// Listening for new Messages
	for {
		select {
		case data := <-flow.Channel():

			// Trying to read message from pub/sub broker
			var message *RedisMessage
			messageRaw := data.Payload
			err := json.Unmarshal([]byte(messageRaw), &message)
			if err != nil {
				continue
			}

			// Getting tunnel from Hub's map
			tunnel, err := hub.FindTunnel(message.Tunnel)
			if err != nil {
				continue
			}

			// Creating message packet for broadcast
			var packet *p.Packet
			err = json.Unmarshal([]byte(message.Data), &packet)
			if err != nil {
				continue
			}

			// Broadcasting message
			for subscriber := range tunnel.Subscribers {
				subscriber.WriteJSON(packet)
			}
		}
	}
}

// Run function creates workers pool, that listen new Messages from broker
func (hub *Hub) Run() {
	flow := hub.Redis.Subscribe("messages")
	for i := 0; i < 10; i++ {
		go hub.worker(flow)
	}

}

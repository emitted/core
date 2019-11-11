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
	Redis   redis.Client
	Tunnels map[string]*t.Tunnel // tunnels map
}

// NewHub is a constructor method for the Hub struct
func NewHub() *Hub {
	return &Hub{
		Tunnels: make(map[string]*t.Tunnel),
	}
}

// existsTunnel function checks tunnel existance in the Hub's Tunnels map
func (hub *Hub) existsTunnel(key string) (tunnel *t.Tunnel, err error) {
	if _, ok := hub.Tunnels[key]; ok {
		return hub.Tunnels[key], nil
	}

	return nil, errors.New("tunnel does not exist yet")
}

// FindTunnel ...
func (hub *Hub) FindTunnel(key string) (*t.Tunnel, error) {
	tunnel, ok := hub.Tunnels[key]
	if ok != true {
		return nil, errors.New("tunnel does not exist")
	}

	return tunnel, nil
}

// GetOrCreateTunnel ...
func (hub *Hub) GetOrCreateTunnel(key string) (*t.Tunnel, error) {
	tunnel, err := t.GetIfExists(key)
	if err != nil {
		return nil, err
	}
	hub.Tunnels[key] = tunnel

	return tunnel, nil
}

func (hub *Hub) worker(flow <-chan *redis.Message) {
	type RedisMessage struct {
		Tunnel string
		Data   json.RawMessage
	}

	for {
		select {
		case data := <-flow:
			var message *RedisMessage
			messageRaw := data.Payload
			err := json.Unmarshal([]byte(messageRaw), &message)
			if err != nil {
				continue
			}

			tunnel, err := hub.FindTunnel(message.Tunnel)
			if err != nil {
				continue
			}

			var packet *p.Packet
			err = json.Unmarshal([]byte(message.Data), &packet)
			if err != nil {
				continue
			}

			for subscriber := range tunnel.Subscribers {
				subscriber.WriteJSON(packet)
			}
		}
	}
}

// Run ...
func (hub *Hub) Run() {
	flow := hub.Redis.Subscribe("messages").Channel()
	for i := 0; i < 10; i++ {
		go hub.worker(flow)
	}
}

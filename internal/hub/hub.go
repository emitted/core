package hub

import (
	"errors"

	"github.com/jinzhu/gorm"
	t "github.com/sireax/emitted/internal/tunnel"
)

var db, err = gorm.Open("mysql", "Sireax:65onion56@/emmet")

// Hub struct contains all data and structs of clients,tunnels etc.
type Hub struct {
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

// GetOrCreateTunnel ...
func (hub *Hub) GetOrCreateTunnel(key string) (*t.Tunnel, error) {
	tunnel, err := t.GetIfExists(key)
	if err != nil {
		return nil, err
	}
	hub.Tunnels[key] = tunnel

	return tunnel, nil
}

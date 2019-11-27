package main

import (
	errs "github.com/sireax/Emmet-Go-Server/internal/errors"
	t "github.com/sireax/Emmet-Go-Server/internal/tunnel"
)

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

// FindTunnel ...
func (hub *Hub) FindTunnel(key string) (*t.Tunnel, error) {
	tunnel, ok := hub.Tunnels[key]
	if ok != true {
		return nil, errs.NewErrTunnelNotFound()
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
	tunnel, err = broker.FindTunnel(key)
	if err != nil {
		return nil, errs.NewErrTunnelNotFound()
	}

	hub.SetTunnel(key, tunnel)

	return tunnel, nil
}

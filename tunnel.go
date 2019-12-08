package main

import (
	"log"
	"sync"
)

// Tunnel structure represents an instance
// of the channel, that clientis connected to.
type Tunnel struct {
	APIkey    string
	Key       string `json:"seckey"`
	Clients   map[uint32]*Client
	Connected int
	Options   *TunnelOptions
	Mux       sync.Mutex
}

// TunnelOptions ...
type TunnelOptions struct {
	Publish        bool `json:"publish"`
	Presence       bool `json:"presence"`
	MaxConnections int  `json:"max_connections"`
}

// NewTunnel is a constructor for Tunnel structure
func NewTunnel(APIKey string, key string, options *TunnelOptions) *Tunnel {
	tunnel := &Tunnel{
		APIkey:    APIKey,
		Key:       key,
		Clients:   make(map[uint32]*Client),
		Connected: 0,
		Options:   options,
	}
	return tunnel
}

// FindOrCreateTunnel ...
func FindOrCreateTunnel(key string) *Tunnel {
	// TODO: THIS FUNCTION IS NOT WORKING
	// TODO: DO NOT USE
	t, _ := hub.Subs[key]

	_, err := hub.AddSub(t)
	if err != nil {
		log.Fatal(err)
	}

	return t
}

// ClientsConnected method counts concurrent connections
func (t *Tunnel) ClientsConnected() int {
	return t.Connected
}

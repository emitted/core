package tunnel

import (
	"sync"

	c "github.com/sireax/Emmet-Go-Server/internal/client"
)

// Tunnel structure represents an instance
// of the channel, that clientis connected to.
type Tunnel struct {
	APIkey         string
	Key            string `json:"seckey"`
	Clients        map[*c.Client]bool
	Connected      int
	MaxConnections int
	Mux            sync.Mutex
}

// NewTunnel is a constructor for Tunnel structure
func NewTunnel(APIKey string, key string, maxConnections int) *Tunnel {
	tunnel := &Tunnel{
		APIkey:         APIKey,
		Key:            key,
		Clients:        make(map[*c.Client]bool),
		Connected:      0,
		MaxConnections: maxConnections,
	}
	return tunnel
}

// ConnectClient ...
func (t *Tunnel) ConnectClient(client *c.Client) {
	t.Mux.Lock()
	t.Clients[client] = true
	t.Connected++
	t.Mux.Unlock()

	client.SendAuthMessage()
}

// DisconnectClient ...
func (t *Tunnel) DisconnectClient(client *c.Client) {
	t.Mux.Lock()
	delete(t.Clients, client)
	t.Connected--
	t.Mux.Unlock()
}

// ClientsConnected method counts concurrent connections
func (t *Tunnel) ClientsConnected() int {
	return t.Connected
}

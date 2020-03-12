package core

import "sync"

// Channel structure represents an instance
// of the channel, that clientproto is connected to.
type Channel struct {
	App  *App
	Name string

	mu      sync.Mutex
	Clients map[string]*Client

	Info map[string][]byte
}

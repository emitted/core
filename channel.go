package core

import "sync"

// Channel structure represents an instance
// of the channel, that client is connected to.
type Channel struct {
	App  *App
	Name string

	mu      sync.Mutex
	Clients map[string]*Client

	infoMu sync.Mutex
	Info   map[string]*ClientInfo
}

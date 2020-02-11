package core

import (
	"context"
	"log"
	"sync"
)

// Hub struct contains all data and structs of clients,channels etc.
type Hub struct {
	mu    sync.RWMutex
	apps  map[string]*App
	conns map[string]*Client
}

// NewHub is a constructor method for the Hub struct
func NewHub() *Hub {
	return &Hub{
		apps: make(map[string]*App),
	}
}

func (hub *Hub) AddApp(app *App) {
	hub.mu.Lock()
	_, ok := hub.apps[app.Secret]
	if !ok {
		hub.apps[app.Secret] = app
	}
	hub.mu.Unlock()
}

func (hub *Hub) BroadcastMessage(appKey string, channelName string, pub *Publication) {

	data, err := pub.Marshal()
	if err != nil {
		log.Fatal(err)
	}

	push := &Push{
		Type: PushTypePublication,
		Data: data,
	}

	payload, err := push.Marshal()

	for _, client := range hub.apps[appKey].Channels[channelName].Clients {
		client.messageWriter.enqueue(payload)
	}
}

func (hub *Hub) BroadcastJoin(appKey string, join *Join) {
	data, err := join.Marshal()
	if err != nil {
		log.Fatal(err)
	}

	push := &Push{
		Type: PushTypeJoin,
		Data: data,
	}

	payload, err := push.Marshal()
	if err != nil {
		log.Fatal(err)
	}

	for _, client := range hub.apps[appKey].Channels[join.Channel].Clients {
		client.messageWriter.enqueue(payload)
	}
}

func (hub *Hub) BroadcastLeave(appKey string, leave *Leave) {
	data, err := leave.Marshal()
	if err != nil {
		log.Fatal(err)
	}

	push := &Push{
		Type: PushTypeLeave,
		Data: data,
	}

	payload, err := push.Marshal()
	if err != nil {
		log.Fatal(err)
	}

	_, ok := hub.apps[appKey]
	if !ok {
		return
	}

	_, ok = hub.apps[appKey].Channels[leave.Channel]
	if !ok {
		return
	}

	for _, client := range hub.apps[appKey].Channels[leave.Channel].Clients {
		client.messageWriter.enqueue(payload)
	}
}

func (h *Hub) shutdown(ctx context.Context) error {
	advice := DisconnectShutdown

	sem := make(chan struct{}, 128)

	clients := make([]*Client, 0, len(h.conns))
	for _, client := range h.conns {
		clients = append(clients, client)
	}

	closeFinishedCh := make(chan struct{}, len(clients))
	finished := 0

	if len(clients) == 0 {
		return nil
	}

	for _, client := range clients {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		go func(cc *Client) {
			defer func() { <-sem }()
			defer func() { closeFinishedCh <- struct{}{} }()
			cc.Close(advice)
		}(client)
	}

	for {
		select {
		case <-closeFinishedCh:
			finished++
			if finished == len(clients) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (hub *Hub) Channels() []string {
	channels := make([]string, 0)
	for _, app := range hub.apps {
		for ch := range app.Channels {
			channels = append(channels, ch)
		}
	}
	return channels
}

func (h *Hub) NumSubscribers(app, ch string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.apps[app]
	if !ok {
		return 0
	}
	conns := h.apps[app].Channels[ch].Clients
	return len(conns)
}

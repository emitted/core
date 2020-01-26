package main

import (
	"context"
	"errors"
	"log"
	"sync"
)

// Hub struct contains all data and structs of clients,channels etc.
type Hub struct {
	mu             sync.RWMutex
	apps           map[string]*App
	conns          map[string]*Client
	numConnections int
	numClients     int
}

// NewHub is a constructor method for the Hub struct
func NewHub() *Hub {
	return &Hub{
		apps:           make(map[string]*App, 0),
		numClients:     0,
		numConnections: 0,
	}
}

// AddApp ...
func (hub *Hub) AddApp(app *App) {
	_, ok := hub.apps[app.Key]
	if !ok {
		hub.apps[app.Key] = app
	}
}

// FindApp ...
func (hub *Hub) FindApp(secret string) (*App, error) {
	app, ok := hub.apps[secret]

	if !ok {
		return nil, errors.New("application is not found")
	}

	return app, nil
}

// BroadcastMessage ...
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
	//disconnect = DisconnectShutdown
	//
	//clients := make([]*Client, 0, len(h.conns))
	//for _, client := range h.conns {
	//	clients = append(clients, client)
	//}
	//h.mu.RUnlock()
	//
	//h.mu.RLock()
	return nil
}

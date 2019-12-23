package main

import (
	"errors"
)

// Hub struct contains all data and structs of clients,channels etc.
type Hub struct {
	Apps           map[string]*App
	numConnections int
	numClients     int
}

// NewHub is a constructor method for the Hub struct
func NewHub() *Hub {
	return &Hub{
		Apps: make(map[string]*App, 0),
		numClients: 0,
		numConnections: 0,
	}
}

// AddApp ...
func (hub *Hub) AddApp(app *App) {
	_, ok := hub.Apps[app.Key]
	if !ok {
		hub.Apps[app.Key] = app
	}
}

// FindApp ...
func (hub *Hub) FindApp(secret string) (*App, error) {
	app, ok := hub.Apps[secret]

	if !ok {
		return nil, errors.New("application is not found")
	}

	return app, nil
}

// BroadcastMessage ...
func (hub *Hub) BroadcastMessage(appKey string, channelName string, data []byte) {

	message := &Message{
		Channel: channelName,
		Event: "message",
		Data:  string(data),
	}

	for _, client := range hub.Apps[appKey].Channels[channelName].Clients {
		client.MessageWriter.enqueue(message.Encode())
	}
}

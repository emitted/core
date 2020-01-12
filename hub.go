package main

import (
	"errors"
	"log"
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
func (hub *Hub) BroadcastMessage(appKey string, channelName string, pub *Publication) {

	for _, client := range hub.Apps[appKey].Channels[channelName].Clients {
		payload, _ := pub.Marshal()
		log.Println(payload)

		client.messageWriter.enqueue(pub.Data)
	}
}

func (hub *Hub) BroadcastJoin(appKey string, join *Join) {
	for _, client := range hub.Apps[appKey].Clients {

		payload, err := join.Marshal()
		if err != nil {
			return
		}
		client.messageWriter.enqueue(payload)
	}
}

func (hub *Hub) BroadcastLeave(appKey string, leave *Leave) {
	for _, client := range hub.Apps[appKey].Clients {

		payload, err := leave.Marshal()
		if err != nil {
			return
		}
		client.messageWriter.enqueue(payload)
	}
}

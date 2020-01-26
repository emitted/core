package main

import (
	"errors"
	"sync"
)

// app structure represents client's application that allows to easily manage channels

// AppOptions ...
type AppOptions struct {
	mu             sync.Mutex
	MaxConnections int
}

// AppStats ...
type AppStats struct {
	Connections int
	Messages    int
}

// app ...
type App struct {
	Key      string
	Secret   string
	Cluster  string
	Clients  map[uint32]*Client
	Channels map[string]*Channel
	Options  *AppOptions
	Stats    *AppStats
}

func (app *App) addSub(ch string, c *Client) bool {

	first := false
	_, ok := app.Channels[ch]
	if !ok {
		first = true
		channel := &Channel{
			Name:    ch,
			App:     app,
			Clients: map[uint32]*Client{},
			Stats:   &ChannelStats{Connections: 0, Messages: 0},
		}
		app.Channels[ch] = channel
	}

	app.Channels[ch].Clients[c.uid] = c

	return first
}

func (app *App) removeSub(ch string, c *Client) (bool, error) {

	if _, ok := app.Channels[ch]; !ok {
		return false, errors.New("channel does not exist")
	}

	lastClient := false

	delete(app.Channels[ch].Clients, c.uid)

	if len(app.Channels[ch].Clients) == 0 {
		lastClient = true
	}

	if len(app.Channels[ch].Clients) == 0 {
		delete(app.Channels, ch)
	}

	return lastClient, nil
}

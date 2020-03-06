package core

import (
	"errors"
	"github.com/sireax/core/internal/database"
	"sync"
	"time"
)

type AppOptions struct {
	JoinLeave bool
}

type AppStats struct {
	mu          sync.RWMutex
	Connections int
	Messages    int
	Join        int
	Leave       int
}

func (s *AppStats) getConns() int {
	s.mu.RLock()
	conns := s.Connections
	s.mu.RUnlock()

	return conns
}

func (s *AppStats) getMsgs() int {
	s.mu.RLock()
	msgs := s.Messages
	s.mu.RUnlock()

	return msgs
}

func (s *AppStats) IncrementConns() {
	s.mu.Lock()
	s.Connections++
	s.mu.Unlock()
}

func (s *AppStats) DecrementConns() {
	s.mu.Lock()
	s.Connections--
	s.mu.Unlock()
}

func (s *AppStats) IncrementMsgs() {
	s.mu.Lock()
	s.Messages++
	s.mu.Unlock()
}

func (s *AppStats) IncrementJoin() {
	s.mu.Lock()
	s.Join++
	s.mu.Unlock()
}

func (s *AppStats) IncrementLeave() {
	s.mu.Lock()
	s.Leave++
	s.mu.Unlock()
}

type App struct {
	ID     string
	Key    string
	Secret string

	MaxConnections int
	MaxMessages    int

	node *Node

	mu sync.Mutex

	Clients  map[string]*Client
	Channels map[string]*Channel
	Options  AppOptions

	statsMu sync.Mutex

	Stats AppStats
}

func (app *App) CanHaveNewConns() bool {
	if app.Stats.getConns() < app.MaxConnections {
		return true
	}

	return false
}

func GetApp(n *Node, secret string) (*App, error) {
	app, ok := n.hub.apps[secret]
	if !ok {
		dbApp := database.GetAppBySecret(secret)
		err := dbApp.Validate()
		if err != nil {
			return nil, err
		}

		app = NewApp(n, dbApp)
		n.hub.AddApp(app)
	}

	return app, nil
}

func NewApp(n *Node, dbApp database.App) *App {
	app := &App{
		ID:     dbApp.Id,
		Secret: dbApp.Secret,
		Key:    dbApp.Key,

		Clients:  make(map[string]*Client),
		Channels: make(map[string]*Channel),

		Options: AppOptions{
			JoinLeave: dbApp.Options.JoinLeave,
		},
		Stats: AppStats{
			Connections: 0,
			Messages:    0,
		},
	}

	go app.runSync()

	return app
}

func (app *App) runSync() {
	ticker := time.NewTicker(time.Second * 10)

	for {
		select {
		case <-ticker.C:
			database.UpdateAppStats(app.Key, app.Stats.getConns(), app.Stats.getMsgs())
		}
	}

}

func (app *App) addSub(ch string, c *Client, info *ClientInfo) bool {

	first := false

	app.mu.Lock()
	defer app.mu.Unlock()

	_, ok := app.Channels[ch]

	if !ok {
		first = true
		channel, err := app.makeChannel(ch)
		if err != nil {
			return false
		}

		app.Channels[ch] = channel
	}

	app.Channels[ch].Clients[c.uid] = c

	if len(info.Info) > 0 {
		app.Channels[ch].Info[c.uid] = info
	}

	return first
}

func (app *App) removeSub(ch string, c *Client) (bool, error) {

	app.mu.Lock()
	defer app.mu.Unlock()

	if _, ok := app.Channels[ch]; !ok {
		return false, errors.New("channel does not exist")
	}

	lastClient := false

	delete(app.Channels[ch].Clients, c.uid)

	if len(app.Channels[ch].Clients) == 0 {
		lastClient = true

		delete(app.Channels, ch)
	}

	return lastClient, nil
}

func (app *App) makeChannel(name string) (*Channel, error) {

	ch := &Channel{
		Name:    name,
		App:     app,
		Clients: map[string]*Client{},
	}

	switch getChannelType(name) {
	case "private":
	case "presence":
		ch.Info = make(map[string]*ClientInfo)
	default:

	}

	return ch, nil
}

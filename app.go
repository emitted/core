package core

import (
	"errors"
	"github.com/emitted/core/common/tickers"
	"sync"
	"time"
)

type AppStats struct {
	connections int
	messages    int

	deltaConnections int
	deltaMessages    int
}

type AppWebhook struct {
	Url string `bson:"url"`

	Channel     bool `bson:"channel"`
	Presence    bool `bson:"presence"`
	Publication bool `bson:"publication"`
}

type AppOptions struct {
	ClientPublications bool `bson:"client_publications"`
	JoinLeave          bool `bson:"join_leave"`
	//Webhooks           []AppWebhook `bson:"webhooks"`
}

type AppCredentials struct {
	Secret string `bson:"secret"`
	Key    string `bson:"key"`
}

type App struct {
	mu sync.RWMutex

	ID             string           `bson:"_id"`
	Credentials    []AppCredentials `bson:"credentials"`
	MaxConnections int              `bson:"max_connections"`
	MaxMessages    int              `bson:"max_messages"`
	Active         bool             `bson:"active"`

	node *Node

	clients  map[string]*Client
	channels map[string]*Channel

	Options AppOptions `bson:"options"`

	stats AppStats

	shutdown   bool
	shutdownCh chan struct{}
}

func (app *App) canHaveNewConns() bool {

	conns := app.stats.connections
	if app.stats.connections < conns {
		return true
	}

	return false
}

func (app *App) Shutdown(disconnect *Disconnect) {
	app.mu.RLock()
	if app.shutdown {
		app.mu.RUnlock()
		return
	}
	app.shutdown = true
	close(app.shutdownCh)
	app.mu.RUnlock()

	clients := make([]*Client, 0, len(app.clients))

	app.mu.RLock()
	for _, channel := range app.clients {
		clients = append(clients, channel)
	}
	app.mu.RUnlock()

	var wg sync.WaitGroup

	for _, c := range clients {

		wg.Add(1)

		go func(client *Client) {

			err := client.Close(disconnect)
			if err != nil {
				app.node.logger.log(NewLogEntry(LogLevelError, "error closing client on app shutdown", map[string]interface{}{"uid": client.uid, "error": err.Error()}))
			}

			wg.Done()

		}(c)
	}
	wg.Wait()

	app.clearStats()

	app.mu.RLock()
	app.shutdown = false
	app.mu.RUnlock()

}

func (app *App) NotifyShutdown() chan struct{} {
	return app.shutdownCh
}

func (app *App) runStatsUpdate() {

	conns, messages, err := app.node.RetrieveStats(app.ID)
	if err != nil {
		app.node.logger.log(NewLogEntry(LogLevelError, "error getting app stats", map[string]interface{}{"error": err.Error()}))
	}

	app.mu.RLock()
	app.stats.connections = conns
	app.stats.messages = messages
	app.mu.RUnlock()

	ticker := tickers.SetTicker(time.Second * 10)

	for {
		select {
		case <-ticker.C:
			err := app.updateStats()
			if err != nil {
				app.node.logger.log(NewLogEntry(LogLevelError, "error updating app stats", map[string]interface{}{"app": app.ID}))
			}
		case <-app.NotifyShutdown():
			tickers.ReleaseTicker(ticker)
		}
	}

}

func (app *App) updateStats() error {

	app.mu.RLock()
	dConns := app.stats.deltaConnections
	dMessages := app.stats.deltaMessages
	app.mu.RUnlock()

	err := app.node.UpdateAppStats(app.ID, dConns, dMessages)
	if err != nil {
		return err
	}

	conns, messages, err := app.node.RetrieveStats(app.ID)
	if err != nil {
		return err
	}

	app.mu.Lock()
	app.stats.connections = conns
	app.stats.messages = messages

	app.stats.deltaConnections = 0
	app.stats.deltaMessages = 0
	app.mu.Unlock()

	return nil
}

func (app *App) clearStats() {
	err := app.node.ClearAppStats(app.ID)
	if err != nil {
		app.node.logger.log(NewLogEntry(LogLevelError, "error clearing app stats", map[string]interface{}{"error": err.Error()}))
	}
}

func (app *App) addSub(ch string, c *Client) bool {

	first := false

	app.mu.Lock()
	defer app.mu.Unlock()

	_, ok := app.channels[ch]

	if !ok {
		first = true
		channel, err := app.makeChannel(ch)
		if err != nil {
			return false
		}

		app.channels[ch] = channel
	}

	app.channels[ch].clients[c.uid] = c

	return first
}

func (app *App) removeSub(ch string, uid string) (bool, error) {

	app.mu.Lock()
	defer app.mu.Unlock()

	if _, ok := app.channels[ch]; !ok {
		return false, errors.New("channel does not exist")
	}

	lastClient := false

	delete(app.channels[ch].clients, uid)

	if len(app.channels[ch].clients) == 0 {
		lastClient = true

		delete(app.channels, ch)
	}

	return lastClient, nil
}

func (app *App) makeChannel(name string) (*Channel, error) {

	ch := &Channel{
		name:             name,
		app:              app,
		clients:          make(map[string]*Client),
		clientPresenceID: make(map[string]interface{}),
	}

	return ch, nil
}

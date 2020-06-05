package core

import (
	"errors"
	"sync"
	"time"
)

type AppStats struct {
	Connections int
	Messages    int
	Join        int
	Leave       int
}

type AppWebhook struct {
	Url string `bson:"url"`

	Channel     bool `bson:"channel"`
	Presence    bool `bson:"presence"`
	Publication bool `bson:"publication"`
}

type AppOptions struct {
	ClientPublications bool         `bson:"client_publications"`
	JoinLeave          bool         `bson:"join_leave"`
	Webhooks           []AppWebhook `bson:"webhooks"`
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

	Due time.Time `bson:"due"`

	node *Node

	Clients  map[string]*Client
	Channels map[string]*Channel

	Options AppOptions `bson:"options"`

	Stats AppStats

	shutdown bool
}

func (app *App) CanHaveNewConns() bool {
	if app.Stats.Connections < app.MaxConnections {
		return true
	}

	return false
}

func (app *App) Shutdown() {
	app.mu.Lock()
	if app.shutdown {
		app.mu.Unlock()
		return
	}
	app.shutdown = true
	app.mu.Unlock()

	channels := make(map[string]*Channel, len(app.Channels))

	for name, channel := range app.Channels {
		channels[name] = channel
	}

	for name, channel := range channels {
		chId := makeChId(app.ID, name)
		err := app.node.broker.Unsubscribe(chId)
		if err != nil {
			app.node.logger.log(NewLogEntry(LogLevelError, "error unsubscribing channel on app shutdown", map[string]interface{}{"app": app.ID, "error": err.Error()}))
		}

		for _, client := range channel.Clients {
			err := client.Close(DisconnectSubscriptionEnded)
			if err != nil {
				app.node.logger.log(NewLogEntry(LogLevelError, "error closing client on app shutdown", map[string]interface{}{"uid": client.uid, "error": err.Error()}))
			}
		}
	}

}

func (app *App) run() {
	ticker := time.NewTicker(time.Second * 10)

	for {
		select {
		case <-ticker.C:

			app.mu.Lock()
			snapshot := app.Stats
			app.mu.Unlock()

			if !app.checkDue() {

				ticker.Stop()
				app.Shutdown()
				return
			}

			err := app.node.UpdateAppStats(app.ID, snapshot)
			if err != nil {
				ticker.Stop()
				app.node.logger.log(NewLogEntry(LogLevelError, "error updating app stats", map[string]interface{}{"error": err.Error()}))
			}
		}
	}

}

func (app *App) checkDue() bool {
	return app.Due.Sub(time.Now()) > 0
}

func (app *App) addSub(ch string, c *Client) bool {

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

	return first
}

func (app *App) removeSub(ch string, uid string) (bool, error) {

	app.mu.Lock()
	defer app.mu.Unlock()

	if _, ok := app.Channels[ch]; !ok {
		return false, errors.New("channel does not exist")
	}

	lastClient := false

	delete(app.Channels[ch].Clients, uid)

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

	return ch, nil
}

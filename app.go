package core

import (
	"errors"
	"github.com/sireax/core/common/database"
	"sync"
	"time"
)

type AppStats struct {
	mu          sync.RWMutex
	Connections int
	Messages    int
	Join        int
	Leave       int
}

type App struct {
	ID     string
	Key    string
	Secret string

	MaxConnections int
	MaxMessages    int

	Due time.Time

	node *Node

	mu sync.Mutex

	Clients  map[string]*Client
	Channels map[string]*Channel
	Options  database.Options

	Stats AppStats

	DueTime *time.Time

	shutdown bool
}

func (app *App) CanHaveNewConns() bool {
	if app.Stats.Connections < app.MaxConnections {
		return true
	}

	return false
}

func (app *App) Destroy() {
	app.mu.Lock()
	if app.shutdown {
		app.mu.Unlock()
		return
	}
	app.shutdown = true

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
		chId := makeChId(app.Secret, name)
		err := app.node.broker.Unsubscribe(chId)
		if err != nil {
			//	todo
		}

		for _, client := range channel.Clients {
			client.Close(DisconnectSubscriptionEnded)
		}
	}

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

		MaxConnections: dbApp.MaxConnections,
		MaxMessages:    dbApp.MaxMessages,

		Due: dbApp.Due,

		Options: dbApp.Options,
		Stats: AppStats{
			Connections: 0,
			Messages:    0,
		},
	}

	app.node = n

	n.logger.log(NewLogEntry(LogLevelDebug, "shutdown due", map[string]interface{}{"due": app.Due.String()}))

	go app.run()

	return app
}

func (app *App) run() {
	ticker := time.NewTicker(time.Second * 10)

	for {
		select {
		case <-ticker.C:

			app.mu.Lock()
			snapshot := &app.Stats
			app.mu.Unlock()

			if !app.checkDue() {

				ticker.Stop()
				app.Shutdown()
				return
			}

			err := app.node.UpdateAppStats(app.Secret, snapshot)
			if err != nil {
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

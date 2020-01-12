package main

// app structure represents client's application that allows to easily manage channels

// AppOptions ...
type AppOptions struct {
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

// Subscribe ...
func (app *App) Subscribe(channel *Channel) bool {
	isFirst := false
	_, ok := app.Channels[channel.Name]

	if !ok {
		isFirst = true
		app.Channels[channel.Name] = channel
	}

	return isFirst
}

func (app *App) addSub(ch string, c *Client) bool  {

	app.Clients[c.uid] = c

	if _, ok := app.Channels[ch]; ok {
		return true
	}

	return false
}

func (app *App) removeSub(ch string, c *Client) bool  {

	if _, ok := app.Channels[ch]; !ok {
		return true
	}

	if _, ok := app.Channels[ch].Clients[c.uid]; !ok {
		return true
	}

	delete(app.Channels[ch].Clients, c.uid)

	if len(app.Channels[ch].Clients) == 0 {
		delete(app.Channels, ch)
	}

	return false
}

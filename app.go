package main

// App structure represents client's application that allows to easily manage channels

// AppOptions ...
type AppOptions struct {
	MaxConnections int
}

// AppStats ...
type AppStats struct {
	Connections int
	Messages    int
}

// App ...
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

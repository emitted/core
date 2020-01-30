package main

// Channel structure represents an instance
// of the channel, that client is connected to.
type Channel struct {
	App     *App
	Name    string
	Stats   *ChannelStats
	Clients map[string]*Client
	Options *ChannelOptions
	Info    map[string]*ClientInfo
}

// ChannelStats ...
type ChannelStats struct {
	Connections int
	Messages    int
}

// ChannelOptions ...
type ChannelOptions struct {
	Publish             bool `json:"publish"`
	Presence            bool `json:"presence"`
	MaxConnections      int  `json:"max_connections"`
	AllowClientMessages bool `json:"client_messages"`
}

// NewChannel is a constructor for Channel structure
func NewChannel(app *App, name string, options *ChannelOptions) *Channel {
	Channel := &Channel{
		App:     app,
		Name:    name,
		Clients: make(map[string]*Client, 0),
		Stats: &ChannelStats{
			Connections: 0,
			Messages:    0,
		},
		Options: options,
		Info:    make(map[string]Raw),
	}
	return Channel
}

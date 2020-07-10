package core

type Channel struct {
	app  *App
	name string

	clients          map[string]*Client
	clientPresenceID map[string]interface{}
}

func (c *Channel) presenceExists(id string) bool {
	_, ok := c.clientPresenceID[id]

	return ok
}

func (c *Channel) addID(id string) {
	c.clientPresenceID[id] = nil
}

func (c *Channel) removeID(id string) {
	delete(c.clientPresenceID, id)
}

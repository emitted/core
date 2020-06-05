package core

type Channel struct {
	App  *App
	Name string

	Clients map[string]*Client
}

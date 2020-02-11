package database

import "github.com/pkg/errors"

type App struct {
	Id     string `bson:"_id"`
	Key    string `bson:"key"`
	Secret string `bson:"secret"`

	MaxConnections int `bson:"max_connections"`
	MaxMessages    int `bson:"max_messages"`

	Options struct {
		JoinLeave bool `bson:"join_leave"`
	} `bson:"options"`
}

func (app *App) Validate() error {
	if app.Key == "" && app.Secret == "" && app.Id == "" {
		return errors.New("app does not exist")
	}

	return nil
}

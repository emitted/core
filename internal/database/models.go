package database

import (
	"github.com/pkg/errors"
	"time"
)

type Webhook struct {
	Url string `bson:"url"`

	Channel     bool `bson:"channel"`
	Presence    bool `bson:"presence"`
	Publication bool `bson:"publication"`
}

type Options struct {
	ClientPublications bool      `bson:"client_publications"`
	JoinLeave          bool      `bson:"join_leave"`
	Webhooks           []Webhook `bson:"webhooks"`
}

type App struct {
	Id     string `bson:"_id"`
	Key    string `bson:"key"`
	Secret string `bson:"secret"`

	MaxConnections int `bson:"max_connections"`
	MaxMessages    int `bson:"max_messages"`

	Options Options `bson:"options"`

	Due time.Time `bson:"due"`
}

func (app *App) Validate() error {
	if app.Key == "" && app.Secret == "" && app.Id == "" {
		return errors.New("app does not exist")
	}

	return nil
}

package database

type App struct {
	Id     string `bson:"_id"`
	Key    string `bson:"key"`
	Secret string `bson:"secret"`

	MaxConnections int `bson:"max_connections"`
	MaxMessages    int `bson:"max_messages"`

	Options struct {
		JoinLeave bool
	} `bson:"options"`
}

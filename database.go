package core

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"time"
)

type MongoConfig struct {
	Address string

	ConnectionTimeout int
	WriteReadTimeout  int
}

type Mongo struct {
	node           *Node
	session        *mgo.Session
	dbName         string
	collectionName string

	config MongoConfig
}

func NewMongo(n *Node, config MongoConfig) *Mongo {
	return &Mongo{
		node:           n,
		dbName:         "test",
		collectionName: "apps",
		config:         config,
	}
}

func (m *Mongo) executeQuery(s func(*mgo.Collection) error) error {
	c := m.session.Copy().DB(m.dbName).C(m.collectionName)
	return s(c)
}

func (m *Mongo) Run() error {
	address := m.config.Address

	session, err := mgo.Dial(address)
	if err != nil {
		return err
	}

	session.SetMode(mgo.Monotonic, true)

	index := mgo.Index{
		Key: []string{"credentials.secret", "credentials.key"},
	}
	c := session.DB(m.dbName).C(m.collectionName)
	err = c.EnsureIndex(index)
	if err != nil {
		return err
	}

	m.session = session

	ticker := time.NewTicker(time.Minute)
	go func() {
		for {
			select {
			case <-m.node.NotifyShutdown():
				ticker.Stop()
				return
			case <-ticker.C:

				err := m.session.Ping()
				if err != nil {
					m.node.logger.log(NewLogEntry(LogLevelError, "error pinging mongodb", map[string]interface{}{"error": err.Error()}))
				}

			}
		}
	}()

	err = m.session.Ping()
	if err != nil {
		return err
	}

	return nil
}

func (m *Mongo) GetAppBySecret(secret string) (*App, error) {
	var app App

	query := func(c *mgo.Collection) error {
		return c.Find(bson.M{"credentials": bson.M{"$elemMatch": bson.M{"secret": secret}}}).One(&app)
	}

	err := m.executeQuery(query)
	if err != nil {
		return nil, err
	}

	return &app, nil
}

func (m *Mongo) GetAppByKey(key string) (*App, error) {
	var app App

	query := func(c *mgo.Collection) error {
		return c.Find(bson.M{"credentials": bson.M{"$elemMatch": bson.M{"key": key}}}).One(&app)
	}

	err := m.executeQuery(query)
	if err != nil {
		return nil, err
	}

	return &app, nil
}

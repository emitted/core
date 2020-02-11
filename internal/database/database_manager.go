package database

import (
	mgo "gopkg.in/mgo.v2"
	"time"
)

const appsCollectionName = "apps"
const statsCollecitonsName = "appstats"

var (
	mgoSession   *mgo.Session
	databaseName = "test"
	pingInterval = time.Second
)

func getSession() *mgo.Session {
	mgoSession, err := mgo.Dial("localhost")

	if err != nil {
		panic(err)
	}

	return mgoSession.Clone()
}

func executeQuery(collectionName string, s func(*mgo.Collection) error) error {
	session := getSession()
	defer session.Close()
	c := session.DB(databaseName).C(collectionName)
	return s(c)
}

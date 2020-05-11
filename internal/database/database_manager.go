package database

import (
	mgo "gopkg.in/mgo.v2"
)

const appsCollectionName = "apps"
const statsCollecitonsName = "appstats"

var (
	mgoSession   *mgo.Session
	databaseName = "test"
)

func SetupSession() error {
	var err error
	mgoSession, err = mgo.Dial("localhost")
	if err != nil {
		return err
	}

	return nil
}

func getSession() *mgo.Session {
	return mgoSession.Clone()
}

func executeQuery(collectionName string, s func(*mgo.Collection) error) error {
	session := getSession()
	defer session.Close()
	c := session.DB(databaseName).C(collectionName)
	return s(c)
}
